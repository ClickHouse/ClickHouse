#include <Storages/ObjectStorage/DataLakes/Iceberg/ExternalPathResolver.h>

#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Common/RemoteHostFilter.h>
#include <Common/maskSensitiveQueryParameters.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageFactory.h>
#include <IO/S3/URI.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Poco/String.h>
#include <Poco/URI.h>
#include <Poco/Util/MapConfiguration.h>
#include <boost/algorithm/string/replace.hpp>
#include <fmt/format.h>
#include <filesystem>
#include <functional>
#if USE_AWS_S3
#include <Common/ObjectStorageKeyGenerator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/S3/diskSettings.h>
#include <IO/S3/S3Capabilities.h>
#include <IO/S3Settings.h>
#endif
#if USE_AVRO
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#endif
#if USE_AZURE_BLOB_STORAGE
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#endif
#if USE_HDFS
#include <Disks/DiskObjectStorage/ObjectStorages/HDFS/HDFSObjectStorage.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int PATH_ACCESS_DENIED;
}

#if USE_AWS_S3
namespace S3AuthSetting
{
    extern const S3AuthSettingsString access_key_id;
    extern const S3AuthSettingsString secret_access_key;
    extern const S3AuthSettingsString session_token;
    extern const S3AuthSettingsString region;
    extern const S3AuthSettingsS3UriStyle uri_style;
}
#endif

namespace Setting
{
extern const SettingsUInt64 hdfs_replication;
extern const SettingsBool object_storage_propagate_credentials_to_other_storages;
}

namespace
{

#if USE_AVRO
std::string normalizeScheme(const std::string & scheme)
{
    auto scheme_lowercase = Poco::toLower(scheme);

    if (scheme_lowercase == "s3a" || scheme_lowercase == "s3n" || scheme_lowercase == "gs" || scheme_lowercase == "gcs" || scheme_lowercase == "oss")
        scheme_lowercase = "s3";
    else if (scheme_lowercase == "wasb" || scheme_lowercase == "wasbs" || scheme_lowercase == "abfss")
        scheme_lowercase = "abfs";

    return scheme_lowercase;
}

/// Local and HDFS storages are rooted at the table directory; paths outside it need a separate storage.
bool isPrefixScopedScheme(const std::string & normalized_scheme)
{
    return normalized_scheme == "file" || normalized_scheme == "hdfs";
}

/// Object keys are compared literally: `.` and `..` are ordinary key segments there, not directory traversal.
bool keyIsInsidePrefix(const std::string & base_key, const std::string & target_key, bool is_filesystem_path)
{
    if (base_key.empty())
        return true;

    auto base = is_filesystem_path ? std::filesystem::path(base_key).lexically_normal().string() : base_key;
    const auto target = is_filesystem_path ? std::filesystem::path(target_key).lexically_normal().string() : target_key;
    if (base.empty())
        return true;
    if (!base.ends_with('/'))
        base.push_back('/');
    return target.starts_with(base);
}

/// A trailing slash is kept by URI normalization, so only the last segment may be empty.
bool keyHasDotOrEmptySegments(std::string_view key)
{
    size_t begin = 0;
    while (begin < key.size())
    {
        size_t end = key.find('/', begin);
        if (end == std::string_view::npos)
            end = key.size();
        const auto segment = key.substr(begin, end - begin);
        if (segment == "." || segment == ".." || (segment.empty() && end < key.size()))
            return true;
        begin = end + 1;
    }
    return false;
}

std::string factoryTypeForScheme(const std::string & normalized_scheme)
{
    if (normalized_scheme == "s3") return "s3";
    if (normalized_scheme == "abfs") return "azure";
    if (normalized_scheme == "hdfs") return "hdfs";
    if (normalized_scheme == "file") return "local";
    return "";
}

std::optional<AccessTypeObjects::Source> sourceForScheme(const std::string & normalized_scheme)
{
    if (normalized_scheme == "s3" || normalized_scheme == "http" || normalized_scheme == "https")
        return AccessTypeObjects::Source::S3;
    if (normalized_scheme == "abfs")
        return AccessTypeObjects::Source::AZURE;
    if (normalized_scheme == "hdfs")
        return AccessTypeObjects::Source::HDFS;
    if (normalized_scheme == "file")
        return AccessTypeObjects::Source::FILE;
    return std::nullopt;
}

#if USE_AWS_S3
S3::URI parseMetadataS3URI(const std::string & path, const SchemeAuthorityKey & decomposed)
{
    const auto normalized_path = decomposed.scheme == "s3a" || decomposed.scheme == "s3n"
        ? "s3://" + decomposed.authority + "/" + decomposed.key : path;
    /// Metadata keys are already encoded; percent-decoding would change keys such as `col=12%3A00%3A00`.
    return S3::URI(normalized_path, /*allow_archive_path_syntax*/ false,
                   /*keep_presigned_query_parameters*/ true, /*uri_style*/ S3UriStyle::AUTO,
                   /*enable_url_encoding*/ false);
}

/// `s3a` and `s3n` inherit the endpoint; `gs`, `gcs` and `oss` select a provider.
std::string s3ProviderFamily(const std::string & scheme)
{
    const auto scheme_lowercase = Poco::toLower(scheme);
    if (scheme_lowercase == "s3" || scheme_lowercase == "s3a" || scheme_lowercase == "s3n")
        return "s3";
    if (scheme_lowercase == "gs" || scheme_lowercase == "gcs")
        return "gcs";
    if (scheme_lowercase == "oss")
        return "oss";
    return "";
}

std::string s3BaseProviderFamily(const std::string & base_scheme, const std::string & base_endpoint)
{
    if (auto family = s3ProviderFamily(base_scheme); !family.empty())
        return family;
    if (base_endpoint.empty())
        return "";
    const auto endpoint_lowercase = Poco::toLower(base_endpoint);
    if (endpoint_lowercase.contains("storage.googleapis.com"))
        return "gcs";
    if (endpoint_lowercase.contains(".aliyuncs.com"))
        return "oss";
    return "s3";
}

bool s3ProviderFamiliesCompatible(const std::string & base_family, const std::string & target_family)
{
    if (base_family.empty() || target_family.empty())
        return true;
    return base_family == target_family;
}

bool s3URIMatches(
    const S3::URI & target_uri,
    const std::string & base_bucket,
    const std::string & base_endpoint,
    const std::string & target_scheme_normalized,
    bool provider_families_compatible)
{
    bool bucket_matches = (target_uri.bucket == base_bucket);
    bool endpoint_matches = (target_uri.endpoint == base_endpoint);
    bool is_generic_s3_uri = (target_scheme_normalized == "s3") && provider_families_compatible;
    return bucket_matches && (endpoint_matches || is_generic_s3_uri);
}

bool sameEndpoint(const std::string & a, const std::string & b)
{
    SchemeAuthorityKey pa(a);
    SchemeAuthorityKey pb(b);
    if (pa.authority.empty() || pb.authority.empty())
        return false;
    return pa.scheme == pb.scheme && pa.authority == pb.authority;
}
#endif

#if USE_AZURE_BLOB_STORAGE
/// Cloud endpoints encode the account in the host; Azurite encodes it in the first path segment.
std::string azureAccountFromServiceUrl(const std::string & url)
{
    auto scheme_end = url.find("://");
    if (scheme_end == std::string::npos)
        return "";
    auto host_begin = scheme_end + 3;
    auto path_begin = url.find('/', host_begin);
    std::string host = url.substr(host_begin, path_begin == std::string::npos ? std::string::npos : path_begin - host_begin);

    if (host.contains(".core."))
        return Poco::toLower(host.substr(0, host.find('.')));

    if (path_begin == std::string::npos)
        return "";
    auto seg_end = url.find('/', path_begin + 1);
    return Poco::toLower(url.substr(path_begin + 1, seg_end == std::string::npos ? std::string::npos : seg_end - path_begin - 1));
}
#endif

std::pair<std::string, std::string> splitAbfsAuthority(const std::string & authority)
{
    auto at_pos = authority.find('@');
    if (at_pos == std::string::npos)
        return {authority, ""};

    std::string account_name = authority.substr(at_pos + 1);
    if (auto suffix_pos = account_name.find('.'); suffix_pos != std::string::npos)
        account_name = account_name.substr(0, suffix_pos);
    return {authority.substr(0, at_pos), account_name};
}

/// ABFS authorities name the DFS endpoint or only the account; the client needs the Blob endpoint of the same account.
std::string azureServiceUrlFromAbfsAuthority(const std::string & authority)
{
    auto at_pos = authority.find('@');
    if (at_pos == std::string::npos || at_pos + 1 == authority.size())
        return "";

    auto host = Poco::toLower(authority.substr(at_pos + 1));
    if (!host.contains('.'))
        host += ".blob.core.windows.net";
    else if (auto dfs_pos = host.find(".dfs."); dfs_pos != std::string::npos)
        host.replace(dfs_pos, 5, ".blob.");
    return "https://" + host;
}

std::pair<ObjectStoragePtr, std::string> getOrCreateStorage(
    const std::string & cache_key,
    const std::string & key_to_use,
    ExternalStorageCache & external_storages,
    const std::function<ObjectStoragePtr(const std::string &)> & create_fn,
    const std::string & supersedes_prefix = {})
{
    std::lock_guard lock(external_storages.mutex);
    if (auto it = external_storages.storages.find(cache_key); it != external_storages.storages.end())
        return {it->second, key_to_use};

    /// The cache key may contain credentials of the metadata path, e.g. URI userinfo, while the storage name is logged.
    ObjectStoragePtr storage = create_fn("iceberg_external_" + std::to_string(sipHash64(cache_key.data(), cache_key.size())));

    /// Evict older credential generations; in-flight readers retain their own `shared_ptr`.
    if (!supersedes_prefix.empty())
        std::erase_if(external_storages.storages, [&](const auto & entry) { return entry.first.starts_with(supersedes_prefix); });

    external_storages.storages.emplace(cache_key, storage);
    return {storage, key_to_use};
}

std::pair<ObjectStoragePtr, std::string> getOrCreateStorageAndKey(
    const std::string & cache_key,
    const std::string & key_to_use,
    const std::string & storage_type,
    ExternalStorageCache & external_storages,
    const ContextPtr & context,
    std::function<void(Poco::Util::MapConfiguration &, const std::string &)> configure_fn)
{
    return getOrCreateStorage(
        cache_key,
        key_to_use,
        external_storages,
        [&](const std::string & storage_name) -> ObjectStoragePtr
        {
            Poco::AutoPtr<Poco::Util::MapConfiguration> cfg(new Poco::Util::MapConfiguration);
            const std::string config_prefix = "object_storages." + storage_name;

            cfg->setString(config_prefix + ".object_storage_type", storage_type);

            configure_fn(*cfg, config_prefix);

            return ObjectStorageFactory::instance().create(
                storage_name,
                *cfg,
                config_prefix,
                context,
                /*run_access_check*/ false,
                /*run_local_paths_check*/ false,
                RemoteHostFilterCheck::None);
        });
}

bool isAbsolutePath(const std::string & path)
{
    if (path.empty())
        return false;

    SchemeAuthorityKey decomposed{path};
    return !decomposed.scheme.empty() || decomposed.key.starts_with('/');
}

#endif // USE_AVRO

/// Validate RFC 3986 schemes: object keys can contain colons without being URIs.
bool isUriScheme(std::string_view candidate)
{
    if (candidate.empty() || !isAlphaASCII(candidate.front()))
        return false;

    for (char c : candidate.substr(1))
        if (!isAlphaNumericASCII(c) && c != '+' && c != '-' && c != '.')
            return false;

    return true;
}

}

SchemeAuthorityKey::SchemeAuthorityKey(const std::string & uri)
{
    if (uri.empty())
        return;

    if (auto scheme_sep = uri.find("://");
        scheme_sep != std::string_view::npos && isUriScheme(std::string_view(uri).substr(0, scheme_sep)))
    {
        scheme = Poco::toLower(uri.substr(0, scheme_sep));
        auto rest = uri.substr(scheme_sep + 3);

        auto slash = rest.find('/');
        if (slash == std::string_view::npos)
        {
            authority = std::string(rest);
            key = "/";
            return;
        }
        authority = std::string(rest.substr(0, slash));
        if (scheme == "file")
            key = std::string(rest.substr(slash));
        else
            key = std::string(rest.substr(++slash));
        return;
    }

    if (auto colon = uri.find(':');
        colon != std::string_view::npos && colon > 0 && isUriScheme(std::string_view(uri).substr(0, colon)))
    {
        auto after_colon = uri.substr(colon + 1);

        if (!after_colon.empty() && after_colon[0] == '/')
        {
            scheme = Poco::toLower(uri.substr(0, colon));
            authority = "";
            key = std::string(after_colon);
            return;
        }
    }

    key = std::string(uri);
}
#if USE_AVRO
namespace
{
/// Probe mode of `tryResolveObjectStorageForPathImpl`: only the grant is evaluated and no storage is created.
/// Path restrictions are skipped, because they do not affect grants and are enforced when the storage is resolved.
struct PathAccessCheck
{
    bool granted = true;
};
}

static std::optional<std::pair<DB::ObjectStoragePtr, std::string>> tryResolveObjectStorageForPathImpl(
    const std::string & table_location,
    const std::string & path,
    const DB::ObjectStoragePtr & base_storage,
    ExternalStorageCache & external_storages,
    const DB::ContextPtr & context,
    DB::AccessType access_type,
    PathAccessCheck * access_check)
{
    if (!isAbsolutePath(path))
        return std::nullopt;

    auto check_local_path_inside_user_files = [&](const std::string & local_path)
    {
        /// `clickhouse-local` intentionally leaves `user_files_path` unrestricted.
        if (context->getApplicationType() != Context::ApplicationType::SERVER)
            return;

        const auto target_path = std::filesystem::path(local_path).lexically_normal();
        const auto user_files_path = std::filesystem::path(context->getUserFilesPath()).lexically_normal();

        /// The lexical check permits a symlink at the leaf; the canonical check prevents escapes through parent symlinks.
        if (user_files_path.empty()
            || !fileOrSymlinkPathStartsWith(target_path.string(), user_files_path.string())
            || !pathStartsWith(target_path.string(), user_files_path.string()))
            throw DB::Exception(
                DB::ErrorCodes::PATH_ACCESS_DENIED,
                "File URI '{}' is outside of allowed `user_files` path '{}'",
                local_path,
                user_files_path.string());
    };

    /// Grants must match the file that is actually opened, so symlinks are resolved; `user_files` keeps its configured spelling.
    auto local_path_for_grant_check = [&](const std::string & local_path)
    {
        const auto user_files_path = std::filesystem::path(context->getUserFilesPath()).lexically_normal();
        if (context->getApplicationType() != Context::ApplicationType::SERVER || user_files_path.empty())
            return std::filesystem::weakly_canonical(local_path).string();
        return (user_files_path / std::filesystem::relative(local_path, user_files_path)).lexically_normal().string();
    };

    SchemeAuthorityKey table_location_decomposed{table_location};
    SchemeAuthorityKey target_decomposed{path};

    if (target_decomposed.scheme.empty() && target_decomposed.key.starts_with('/'))
    {
        const bool base_is_local = base_storage->getType() == ObjectStorageType::Local;

        if (!base_is_local || keyIsInsidePrefix(table_location_decomposed.key, target_decomposed.key, /*is_filesystem_path*/ true))
            return std::nullopt;

        target_decomposed.scheme = "file";
    }

    const std::string base_scheme_normalized = normalizeScheme(table_location_decomposed.scheme);
    const std::string target_scheme_normalized = normalizeScheme(target_decomposed.scheme);

    /// Runs before any diagnostic that names the external path, host or object version.
    auto check_access = [&]
    {
        /// A background operation has no user whose grants could allow the path.
        if (context->isBackgroundContext())
        {
            if (access_check)
            {
                access_check->granted = false;
                return;
            }
            throw DB::Exception(
                DB::ErrorCodes::ACCESS_DENIED,
                "Iceberg path '{}' outside the table location is not read by background operations",
                maskCredentialsInURI(path));
        }

        const auto source = sourceForScheme(target_scheme_normalized);
        if (!source)
        {
            if (access_check)
                return;
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported storage scheme '{}' in path '{}'", target_scheme_normalized, path);
        }

        /// Object keys are used verbatim, while the grant check percent-decodes URIs, so escape `%` to keep the checked key exact.
        /// The grant check also drops empty, `.` and `..` segments, so keys with them can only match an unfiltered grant.
        std::string uri_to_check;
        if (target_scheme_normalized == "file")
            uri_to_check = local_path_for_grant_check(target_decomposed.key);
        else if (!keyHasDotOrEmptySegments(target_decomposed.key))
            uri_to_check = boost::replace_all_copy(path, "%", "%25");
        if (access_check)
            access_check->granted = context->getAccess()->isGrantedWithFilter(
                access_type, AccessTypeObjects::toStringSource(*source), uri_to_check);
        else
            context->getAccess()->checkAccessWithFilter(
                access_type, AccessTypeObjects::toStringSource(*source), uri_to_check);
    };

    if (target_scheme_normalized == "file")
    {
        /// RFC 8089 treats `localhost` as an empty authority.
        std::string remote_authority;
        if (!target_decomposed.authority.empty() && Poco::toLower(target_decomposed.authority) != "localhost")
            remote_authority = target_decomposed.authority;
        target_decomposed.authority.clear();

        if (!remote_authority.empty() && !access_check)
        {
            check_access();
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "File URI '{}' refers to host '{}', but `file://` paths are always read from the local "
                "filesystem. Only an empty authority (`file:///path`) or `localhost` is supported",
                maskCredentialsInURI(path),
                remote_authority);
        }

        /// Re-rooted at the queried table directory, where the base storage validates the translated path.
        if (base_scheme_normalized == "file" && table_location_decomposed.authority == target_decomposed.authority
            && keyIsInsidePrefix(table_location_decomposed.key, target_decomposed.key, /*is_filesystem_path*/ true))
            return std::nullopt;

    }

    auto check_remote_host = [&](const std::string & url)
    {
        context->getGlobalContext()->getRemoteHostFilter().checkURL(Poco::URI(url));
    };

    #if USE_AWS_S3
    if (target_scheme_normalized == "s3" || target_scheme_normalized == "https" || target_scheme_normalized == "http")
    {
        /// A path spelled under `location` is re-rooted below. Checked before parsing, which is costly per file.
        /// A query may pin an object version, which is rejected below.
        if (path.size() > table_location.size() && path.starts_with(table_location)
            && (table_location.ends_with('/') || path[table_location.size()] == '/')
            && path.find_first_of("?#", table_location.size()) == std::string::npos)
            return std::nullopt;

        auto s3_uri = parseMetadataS3URI(path, target_decomposed);

        /// Storages are shared per bucket and read the latest object version, so a pinned version cannot be honored.
        if (!s3_uri.version_id.empty() && !access_check)
        {
            check_access();
            throw DB::Exception(
                DB::ErrorCodes::NOT_IMPLEMENTED,
                "Iceberg metadata references a specific version of S3 object '{}', reading S3 object versions is not supported",
                maskCredentialsInURI(path));
        }

        std::string key_to_use = s3_uri.key;

        std::string base_storage_endpoint;
        if (base_storage->getType() == ObjectStorageType::S3)
            base_storage_endpoint = base_storage->getDescription();

        const bool provider_families_compatible = s3ProviderFamiliesCompatible(
            s3BaseProviderFamily(table_location_decomposed.scheme, base_storage_endpoint),
            s3ProviderFamily(target_decomposed.scheme));

        /// A table copied without rewriting metadata still uses its old `location`; re-root paths inside it.
        if (base_scheme_normalized == "s3" || base_scheme_normalized == "https" || base_scheme_normalized == "http")
        {
            auto base_s3_uri = parseMetadataS3URI(table_location, table_location_decomposed);

            if (s3URIMatches(s3_uri, base_s3_uri.bucket, base_s3_uri.endpoint, target_scheme_normalized, provider_families_compatible)
                && keyIsInsidePrefix(base_s3_uri.key, key_to_use, /*is_filesystem_path*/ false))
                return std::nullopt;
        }

        if (base_storage->getType() == ObjectStorageType::S3)
        {
            if (auto s3_storage = std::dynamic_pointer_cast<S3ObjectStorage>(base_storage))
            {
                const std::string base_bucket = s3_storage->getObjectsNamespace();
                const std::string base_endpoint = s3_storage->getDescription();

                if (s3URIMatches(s3_uri, base_bucket, base_endpoint, target_scheme_normalized, provider_families_compatible))
                {
                    check_access();
                    if (access_check != nullptr)
                        return std::nullopt;
                    return std::make_pair(base_storage, key_to_use);
                }
            }
        }

        const bool endpoint_explicit = (target_decomposed.scheme == "http" || target_decomposed.scheme == "https");

        std::string endpoint_to_use;

        S3UriStyle endpoint_uri_style = S3UriStyle::AUTO;

        auto make_endpoint_with_bucket = [&]() -> std::string
        {
            if (s3_uri.endpoint.empty())
                return "https://" + s3_uri.bucket + ".s3.amazonaws.com";

            const auto scheme_end = s3_uri.endpoint.find("://");
            if (s3_uri.is_virtual_hosted_style && scheme_end != std::string::npos)
            {
                return s3_uri.endpoint.substr(0, scheme_end + 3) + s3_uri.bucket + "." + s3_uri.endpoint.substr(scheme_end + 3);
            }

            endpoint_uri_style = S3UriStyle::PATH;
            return s3_uri.endpoint + "/" + s3_uri.bucket;
        };

        if (endpoint_explicit || !provider_families_compatible)
        {
            endpoint_to_use = make_endpoint_with_bucket();
        }
        else
        {
            std::string base_endpoint;
            if (base_storage->getType() == ObjectStorageType::S3)
                base_endpoint = base_storage->getDescription();

            if (!base_endpoint.empty())
            {
                if (base_endpoint.contains(".s3.") && base_endpoint.contains(".amazonaws.com"))
                {
                    size_t s3_pos = base_endpoint.find(".s3.");
                    size_t scheme_end = base_endpoint.find("://");
                    if (scheme_end != std::string::npos)
                    {
                        std::string scheme = base_endpoint.substr(0, scheme_end + 3);
                        std::string suffix = base_endpoint.substr(s3_pos);

                        size_t slash_pos = suffix.find('/', 1);
                        if (slash_pos != std::string::npos)
                            suffix = suffix.substr(0, slash_pos);
                        endpoint_to_use = scheme + s3_uri.bucket + suffix;
                    }
                }
                else
                {
                    size_t scheme_end = base_endpoint.find("://");
                    if (scheme_end != std::string::npos)
                    {
                        size_t path_start = base_endpoint.find('/', scheme_end + 3);
                        if (path_start != std::string::npos)
                            base_endpoint = base_endpoint.substr(0, path_start);
                    }
                    if (!base_endpoint.empty() && base_endpoint.back() == '/')
                        base_endpoint.pop_back();
                    endpoint_to_use = base_endpoint + "/" + s3_uri.bucket;
                    endpoint_uri_style = S3UriStyle::PATH;
                }
            }

            if (endpoint_to_use.empty())
                endpoint_to_use = make_endpoint_with_bucket();
        }

        check_access();
        if (access_check != nullptr)
            return std::nullopt;
        S3::URI(endpoint_to_use, /*allow_archive_path_syntax*/ false, /*keep_presigned_query_parameters*/ true, endpoint_uri_style)
            .checkRemoteHostFilter(context->getGlobalContext()->getRemoteHostFilter());

        /// Include session restrictions in the cache identity to prevent reuse of a privileged client.
        const bool restricts_server_credentials = context->shouldRestrictUserQueryS3Credentials();

        /// Propagating credentials must not bypass the session restriction on server-managed credentials.
        const bool propagate_creds = !restricts_server_credentials
            && context->getSettingsRef()[Setting::object_storage_propagate_credentials_to_other_storages];

        bool reuse_base_credentials = false;
        if (base_storage->getType() == ObjectStorageType::S3)
            reuse_base_credentials = propagate_creds
                || (provider_families_compatible
                    && (!endpoint_explicit || sameEndpoint(base_storage->getDescription(), s3_uri.endpoint)));

        String access_key_id;
        String secret_access_key;
        String session_token;
        String region;
        /// Bearer token of catalog-vended GCS credentials; the base client is rebuilt with a new one on refresh.
        String authorization_header;
        std::optional<S3Settings> base_s3_settings;
        if (reuse_base_credentials)
        {
            if (auto s3_storage = std::dynamic_pointer_cast<S3ObjectStorage>(base_storage))
            {
                base_s3_settings.emplace(s3_storage->getS3Settings());

                if (auto s3_client = s3_storage->tryGetS3StorageClient())
                {
                    const auto credentials = s3_client->getCredentials();
                    access_key_id = credentials.GetAWSAccessKeyId();
                    secret_access_key = credentials.GetAWSSecretKey();
                    session_token = credentials.GetSessionToken();
                    region = s3_client->getRegion();

                    for (const auto & header : s3_client->getClientConfiguration().extra_headers)
                        if (header.name == "authorization")
                            authorization_header = header.value;
                }
            }
        }

        std::string storage_cache_key = "s3://" + s3_uri.bucket + "@" + endpoint_to_use
            + "#propagate=" + (propagate_creds ? "1" : "0")
            + "#restrict=" + (restricts_server_credentials ? "1" : "0");

        /// Credential rotation makes older cache entries unreachable; evict them by their common prefix.
        std::string supersedes_prefix;
        if (!access_key_id.empty() || !session_token.empty() || !authorization_header.empty())
        {
            supersedes_prefix = storage_cache_key + "#cred=";

            SipHash creds_hash;
            creds_hash.update(access_key_id);
            creds_hash.update(secret_access_key);
            creds_hash.update(session_token);
            creds_hash.update(authorization_header);
            storage_cache_key = supersedes_prefix + std::to_string(creds_hash.get64());
        }

        return getOrCreateStorage(
            storage_cache_key,
            key_to_use,
            external_storages,
            [&](const std::string & storage_name) -> ObjectStoragePtr
            {
                S3Settings s3_settings;
                if (base_s3_settings)
                    s3_settings = S3Settings(*base_s3_settings);
                else
                {
                    Poco::AutoPtr<Poco::Util::MapConfiguration> cfg(new Poco::Util::MapConfiguration);
                    const std::string config_prefix = "object_storages." + storage_name;
                    cfg->setString(config_prefix + ".endpoint", endpoint_to_use);

                    s3_settings.loadFromConfigForObjectStorage(
                        *cfg, config_prefix, context->getSettingsRef(), Poco::URI(endpoint_to_use).getScheme(),
                        /*validate_settings*/ true);
                }

                /// Use the credential generation captured in the cache key.
                /// These are the base client's final credentials, already of the assumed role, so do not assume it again.
                if (!access_key_id.empty())
                {
                    s3_settings.auth_settings.clearRoleArn();
                    s3_settings.auth_settings[S3AuthSetting::access_key_id] = access_key_id;
                }
                if (!secret_access_key.empty())
                    s3_settings.auth_settings[S3AuthSetting::secret_access_key] = secret_access_key;
                if (!session_token.empty())
                    s3_settings.auth_settings[S3AuthSetting::session_token] = session_token;
                if (!region.empty())
                    s3_settings.auth_settings[S3AuthSetting::region] = region;
                if (!authorization_header.empty())
                {
                    s3_settings.auth_settings.headers.eraseByName("Authorization");
                    s3_settings.auth_settings.headers.push_back({"Authorization", authorization_header});
                }

                s3_settings.auth_settings[S3AuthSetting::uri_style] = endpoint_uri_style;

                S3::URI storage_uri(
                    endpoint_to_use, /*allow_archive_path_syntax*/ false, /*keep_presigned_query_parameters*/ true,
                    s3_settings.auth_settings[S3AuthSetting::uri_style]);

                if (!storage_uri.key.empty() && !storage_uri.key.ends_with('/'))
                    storage_uri.key.push_back('/');

                /// `for_disk_s3 = false` enforces session restrictions on server credentials.
                auto client = getClient(endpoint_to_use, s3_settings, context, /*for_disk_s3*/ false, storage_name);

                return std::make_shared<S3ObjectStorage>(
                    std::move(client),
                    std::make_unique<S3Settings>(std::move(s3_settings)),
                    storage_uri,
                    S3Capabilities{},
                    createObjectStorageKeyGeneratorByPrefix(storage_uri.key),
                    storage_name);
            },
            supersedes_prefix);
    }
    #endif

    #if USE_HDFS
    /// `HDFSObjectStorage` resolves a relative key against the user's home directory, so the key keeps the leading `/`.
    const std::string hdfs_key = "/" + target_decomposed.key;

    if (target_scheme_normalized == "hdfs")
    {
        bool use_base_storage = false;

        if (base_storage->getType() == ObjectStorageType::HDFS)
        {
            if (auto hdfs_storage = std::dynamic_pointer_cast<HDFSObjectStorage>(base_storage))
            {
                const std::string base_url = hdfs_storage->getDescription();
                std::string base_endpoint;
                if (auto pos = base_url.find('/', base_url.find("//") + 2); pos != std::string::npos)
                    base_endpoint = base_url.substr(0, pos);
                else
                    base_endpoint = base_url;

                std::string target_endpoint = target_scheme_normalized + "://" + target_decomposed.authority;

                const bool inside_table_prefix
                    = keyIsInsidePrefix(table_location_decomposed.key, target_decomposed.key, /*is_filesystem_path*/ true);

                /// A table copied without rewriting metadata still uses its old `location`; re-root paths inside it.
                if (base_scheme_normalized == "hdfs" && inside_table_prefix
                    && table_location_decomposed.authority == target_decomposed.authority)
                    return std::nullopt;

                if (base_endpoint == target_endpoint && inside_table_prefix)
                    use_base_storage = true;
            }
        }

        if (use_base_storage)
        {
            check_access();
            if (access_check != nullptr)
                return std::nullopt;
            return std::make_pair(base_storage, hdfs_key);
        }
    }
    #endif

    /// Paths inside `location` are re-rooted at the queried table directory. A path outside it gets the storage of its own
    /// backend below: the base storage may be another backend than `location` names, e.g. for a copied table.
    if (base_scheme_normalized == target_scheme_normalized && table_location_decomposed.authority == target_decomposed.authority
        && keyIsInsidePrefix(
            table_location_decomposed.key, target_decomposed.key, /*is_filesystem_path*/ isPrefixScopedScheme(target_scheme_normalized)))
        return std::nullopt;

    const std::string type_for_factory = factoryTypeForScheme(target_scheme_normalized);
    if (type_for_factory.empty())
    {
        if (access_check)
            return std::nullopt;
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported storage scheme '{}' in path '{}'", target_scheme_normalized, path);
    }

    /// Each local storage is rooted at one parent directory, so the directory must be part of its cache key.
    std::string file_dir_path;
    std::string cache_key;
    if (target_scheme_normalized == "file")
    {
        std::filesystem::path dir_path = std::filesystem::path(target_decomposed.key).parent_path();
        /// The local storage creates its root directory, which a read must not do. A missing directory holds no files,
        /// so the storage is rooted at the nearest existing ancestor; keys are absolute paths, so they do not change.
        if (!access_check && access_type == DB::AccessType::READ)
            while (dir_path.has_relative_path() && !std::filesystem::is_directory(dir_path))
                dir_path = dir_path.parent_path();
        file_dir_path = dir_path.string();
        if (file_dir_path.empty() || file_dir_path == "/")
            file_dir_path = "/";
        else if (file_dir_path.back() != '/')
            file_dir_path += '/';
        cache_key = "file://" + file_dir_path;
    }
    else
    {
        cache_key = target_scheme_normalized + "://" + target_decomposed.authority;
    }

#if USE_AZURE_BLOB_STORAGE
    if (target_scheme_normalized == "abfs" && base_storage->getType() == ObjectStorageType::Azure)
    {
        if (auto azure_storage = std::dynamic_pointer_cast<AzureObjectStorage>(base_storage))
        {
            const auto [container_name, account_name] = splitAbfsAuthority(target_decomposed.authority);
            const auto & conn_params = azure_storage->getConnectionParameters();

            check_access();
            if (access_check != nullptr)
                return std::nullopt;

            const std::string target_account = Poco::toLower(account_name);
            const std::string base_account = azureAccountFromServiceUrl(conn_params.getConnectionURL());

            if (!target_account.empty() && !base_account.empty() && target_account != base_account)
                throw DB::Exception(
                    DB::ErrorCodes::BAD_ARGUMENTS,
                    "Iceberg metadata references Azure storage account '{}', which differs from the table's "
                    "base account '{}'. Reading across Azure accounts is not supported; configure access to "
                    "account '{}'.",
                    account_name, base_account, account_name);

            /// An authority with a host names the endpoint as well, e.g. of another cloud, which the cloned client would not reach.
            const auto at_pos = target_decomposed.authority.find('@');
            const bool authority_names_host = at_pos != std::string::npos && target_decomposed.authority.find('.', at_pos) != std::string::npos;
            if (authority_names_host)
            {
                const auto target_endpoint
                    = Poco::toLower(Poco::URI(azureServiceUrlFromAbfsAuthority(target_decomposed.authority)).getAuthority());
                const auto base_endpoint = Poco::toLower(Poco::URI(conn_params.getConnectionURL()).getAuthority());
                if (target_endpoint != base_endpoint)
                    throw DB::Exception(
                        DB::ErrorCodes::BAD_ARGUMENTS,
                        "Iceberg metadata references Azure endpoint '{}', which differs from the table's base endpoint '{}'. "
                        "Reading across Azure endpoints is not supported.",
                        target_endpoint, base_endpoint);
            }

            check_remote_host(conn_params.getConnectionURL());

            /// A credential rotation publishes new connection parameters of the base storage; evict clients built from older ones.
            /// The cached clone keeps the credential object alive, so its address cannot be reused by a newer credential.
            SipHash auth_hash;
            auth_hash.update(conn_params.endpoint.storage_account_url);
            auth_hash.update(conn_params.endpoint.account_name);
            auth_hash.update(conn_params.endpoint.account_key);
            auth_hash.update(conn_params.endpoint.sas_auth);
            auth_hash.update(conn_params.endpoint.additional_params);
            auth_hash.update(conn_params.auth_method.index());
            std::visit([&]<typename T>(const T & auth)
            {
                if constexpr (!std::is_same_v<T, AzureBlobStorage::ConnectionString>)
                    auth_hash.update(reinterpret_cast<uintptr_t>(auth.get()));
            }, conn_params.auth_method);
            const std::string supersedes_prefix = cache_key + "#auth=";

            /// Clone connection parameters to preserve SAS and workload identity authentication, which config cannot express.
            auto storage_and_key = getOrCreateStorage(
                supersedes_prefix + std::to_string(auth_hash.get64()),
                target_decomposed.key,
                external_storages,
                [&](const std::string & storage_name) -> ObjectStoragePtr
                {
                    auto params = conn_params;
                    params.endpoint.container_name = container_name;
                    params.endpoint.prefix.clear();

                    auto client = AzureBlobStorage::getContainerClient(params, /*readonly*/ true);

                    return std::make_shared<AzureObjectStorage>(
                        storage_name,
                        std::move(client),
                        AzureBlobStorage::getRequestSettings(context->getSettingsRef()),
                        params,
                        params.getContainer(),
                        /// `getServiceEndpoint` would expose the SAS token in the storage description.
                        params.getConnectionURL(),
                        /*common_key_prefix*/ "");
                },
                supersedes_prefix);

            /// The cached clone may have been built for a session that allows the server's own identity.
            checkSessionMayUseServerIdentity(*storage_and_key.first, context);
            return storage_and_key;
        }
    }
#endif

    check_access();
    if (access_check != nullptr)
        return std::nullopt;

    if (target_scheme_normalized == "file")
        check_local_path_inside_user_files(target_decomposed.key);

    std::string azure_service_url;
    if (target_scheme_normalized == "abfs")
    {
        azure_service_url = azureServiceUrlFromAbfsAuthority(target_decomposed.authority);
        if (azure_service_url.empty())
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS, "Azure path '{}' does not specify a storage account", maskCredentialsInURI(path));
    }

    /// The client built below has no credentials, so it would use the server's own identity. Checked before the cache
    /// lookup, because a cached client may have been built for a session that allows it.
    if (target_scheme_normalized == "abfs" && context->shouldRestrictUserQueryS3Credentials())
        throw DB::Exception(
            DB::ErrorCodes::ACCESS_DENIED,
            "Iceberg metadata references Azure path '{}' outside the table's storage, which can only be read with the server's own "
            "identity. User queries are not allowed to use it (`s3_allow_server_credentials_in_user_queries` = 0)",
            maskCredentialsInURI(path));

    if (!azure_service_url.empty())
        check_remote_host(azure_service_url);
    else if (!target_decomposed.authority.empty())
        check_remote_host(target_scheme_normalized + "://" + target_decomposed.authority + "/");

#if USE_HDFS
    /// `HDFSObjectStorage` keeps a reference to its configuration, so it gets the server configuration rather than a temporary one.
    if (target_scheme_normalized == "hdfs")
    {
        return getOrCreateStorage(
            cache_key,
            hdfs_key,
            external_storages,
            [&](const std::string & storage_name) -> ObjectStoragePtr
            {
                auto hdfs_settings = std::make_unique<HDFSObjectStorageSettings>(
                    /*min_bytes_for_seek*/ 1024 * 1024, context->getSettingsRef()[Setting::hdfs_replication]);
                return std::make_shared<HDFSObjectStorage>(
                    cache_key + "/", std::move(hdfs_settings), context->getConfigRef(), /*lazy_initialize*/ false, storage_name);
            });
    }
#endif

    return getOrCreateStorageAndKey(
        cache_key,
        target_decomposed.key,
        type_for_factory,
        external_storages,
        context,
        [&](Poco::Util::MapConfiguration & cfg, const std::string & config_prefix)
        {
            if (target_scheme_normalized == "file")
            {
                cfg.setString(config_prefix + ".path", file_dir_path);
            }
            else if (target_scheme_normalized == "abfs")
            {
                const auto [container_name, account_name] = splitAbfsAuthority(target_decomposed.authority);

                cfg.setString(config_prefix + ".storage_account_url", azure_service_url);
                cfg.setString(config_prefix + ".container_name", container_name);
                /// Reads must not create a missing container.
                cfg.setBool(config_prefix + ".container_already_exists", true);
                if (!account_name.empty())
                    cfg.setString(config_prefix + ".account_name", account_name);
            }
        });
}

static std::optional<std::pair<DB::ObjectStoragePtr, std::string>> tryResolveObjectStorageForPath(
    const std::string & table_location,
    const std::string & path,
    const DB::ObjectStoragePtr & base_storage,
    ExternalStorageCache & external_storages,
    const DB::ContextPtr & context)
{
    return tryResolveObjectStorageForPathImpl(
        table_location, path, base_storage, external_storages, context, DB::AccessType::READ, /*access_check*/ nullptr);
}

static PathAccessCheck checkPathAccess(
    const std::string & table_location,
    const std::string & path,
    const DB::ObjectStoragePtr & base_storage,
    const DB::ContextPtr & context,
    DB::AccessType access_type)
{
    ExternalStorageCache unused_storages;
    PathAccessCheck access_check;
    tryResolveObjectStorageForPathImpl(table_location, path, base_storage, unused_storages, context, access_type, &access_check);
    return access_check;
}

bool isPathReadGranted(
    const std::string & table_location,
    const std::string & path,
    const DB::ObjectStoragePtr & base_storage,
    const DB::ContextPtr & context)
{
    return checkPathAccess(table_location, path, base_storage, context, DB::AccessType::READ).granted;
}

bool isPathWriteGranted(
    const std::string & table_location,
    const std::string & path,
    const DB::ObjectStoragePtr & base_storage,
    const DB::ContextPtr & context)
{
    return checkPathAccess(table_location, path, base_storage, context, DB::AccessType::WRITE).granted;
}

void checkPathReadable(
    const std::string & table_location,
    const std::string & path,
    const DB::ObjectStoragePtr & base_storage,
    ExternalStorageCache & external_storages,
    const DB::ContextPtr & context)
{
    tryResolveObjectStorageForPath(table_location, path, base_storage, external_storages, context);
}

std::pair<DB::ObjectStoragePtr, std::string> resolveObjectStorageForPath(
    const std::string & table_location,
    const std::string & path,
    const DB::ObjectStoragePtr & base_storage,
    ExternalStorageCache & external_storages,
    const DB::ContextPtr & context,
    const Iceberg::IcebergPathResolver & path_resolver)
{
    if (auto resolved = tryResolveObjectStorageForPath(table_location, path, base_storage, external_storages, context))
        return *resolved;
    return {base_storage, path_resolver.resolve(Iceberg::IcebergPathFromMetadata::deserialize(path))};
}

std::pair<DB::ObjectStoragePtr, std::string> resolveObjectStorageForWrite(
    const std::string & table_location,
    const std::string & path,
    const DB::ObjectStoragePtr & base_storage,
    ExternalStorageCache & external_storages,
    const DB::ContextPtr & context,
    const Iceberg::IcebergPathResolver & path_resolver)
{
    if (auto resolved = tryResolveObjectStorageForPathImpl(
            table_location, path, base_storage, external_storages, context, DB::AccessType::WRITE, /*access_check*/ nullptr))
        return *resolved;
    return {base_storage, path_resolver.resolve(Iceberg::IcebergPathFromMetadata::deserialize(path))};
}

bool isObjectInTableDirectory(
    const ObjectStoragePtr & storage,
    const std::string & key,
    const ObjectStoragePtr & table_storage,
    const std::string & table_path)
{
    if (storage != table_storage || !key.starts_with(table_path))
        return false;
    return table_path.empty() || table_path.ends_with('/')
        || (key.size() > table_path.size() && key[table_path.size()] == '/');
}

void resolveObjectStorageFromDataLakeMetadata(
    const ObjectInfoPtr & object,
    const std::string & table_location,
    const ObjectStoragePtr & base_storage,
    ExternalStorageCache & external_storages,
    const ContextPtr & context)
{
    auto iceberg_info = std::dynamic_pointer_cast<IcebergDataObjectInfo>(object);
    if (!iceberg_info || iceberg_info->tryGetResolvedStorage())
        return;

    auto metadata_path = iceberg_info->getPathInDataLakeMetadata();
    if (!metadata_path)
        return;

    /// Base-storage tasks already carry the key resolved by the coordinator.
    if (auto resolved = tryResolveObjectStorageForPath(table_location, *metadata_path, base_storage, external_storages, context);
        resolved && resolved->first != base_storage)
    {
        iceberg_info->setResolvedStorage(resolved->first);
        iceberg_info->relative_path_with_metadata.relative_path = resolved->second;
    }
}

#endif

}
