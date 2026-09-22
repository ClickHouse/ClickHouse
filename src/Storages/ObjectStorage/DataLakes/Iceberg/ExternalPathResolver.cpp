#include <Storages/ObjectStorage/DataLakes/Iceberg/ExternalPathResolver.h>

#include <Access/Common/AccessType.h>
#include <Access/Common/normalizeAccessURI.h>
#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Common/RemoteHostFilter.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageFactory.h>
#include <IO/S3/URI.h>
#include <Interpreters/Context.h>
#include <Poco/String.h>
#include <Poco/URI.h>
#include <Poco/Util/MapConfiguration.h>
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
    extern const int BAD_ARGUMENTS;
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

bool keyIsInsidePrefix(const std::string & base_key, const std::string & target_key)
{
    if (base_key.empty())
        return true;

    auto base = std::filesystem::path(base_key).lexically_normal().string();
    const auto target = std::filesystem::path(target_key).lexically_normal().string();
    if (base.empty())
        return true;
    if (!base.ends_with('/'))
        base.push_back('/');
    return target.starts_with(base);
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
    /// Normalize only Hadoop aliases; other schemes select their own provider in `S3::URI`.
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

void checkRemoteHostIsAllowed(const ContextPtr & context, const std::string & url)
{
    context->getGlobalContext()->getRemoteHostFilter().checkURL(Poco::URI(url));
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

    ObjectStoragePtr storage = create_fn(cache_key);

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
                storage_name, *cfg, config_prefix, context, /*run_access_check*/ false, /*run_local_paths_check*/ false);
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
struct PathReadCheck
{
    bool granted = true;
    bool openable = true;
};
}

static std::optional<std::pair<DB::ObjectStoragePtr, std::string>> tryResolveObjectStorageForPathImpl(
    const std::string & table_location,
    const std::string & path,
    const DB::ObjectStoragePtr & base_storage,
    ExternalStorageCache & external_storages,
    const DB::ContextPtr & context,
    PathReadCheck * read_check)
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
        {
            if (read_check)
                read_check->openable = false;
            else
                throw DB::Exception(
                    DB::ErrorCodes::PATH_ACCESS_DENIED,
                    "File URI '{}' is outside of allowed `user_files` path '{}'",
                    local_path,
                    user_files_path.string());
        }
    };

    SchemeAuthorityKey table_location_decomposed{table_location};
    SchemeAuthorityKey target_decomposed{path};

    if (target_decomposed.scheme.empty() && target_decomposed.key.starts_with('/'))
    {
        const bool base_is_local = base_storage->getType() == ObjectStorageType::Local;

        if (!base_is_local || keyIsInsidePrefix(table_location_decomposed.key, target_decomposed.key))
            return std::nullopt;

        target_decomposed.scheme = "file";
    }

    const std::string base_scheme_normalized = normalizeScheme(table_location_decomposed.scheme);
    const std::string target_scheme_normalized = normalizeScheme(target_decomposed.scheme);

    if (target_scheme_normalized == "file")
    {
        /// RFC 8089 treats `localhost` as an empty authority.
        std::string remote_authority;
        if (!target_decomposed.authority.empty() && Poco::toLower(target_decomposed.authority) != "localhost")
            remote_authority = target_decomposed.authority;
        target_decomposed.authority.clear();

        if (!remote_authority.empty())
        {
            if (read_check)
                read_check->openable = false;
            else
                throw DB::Exception(
                    DB::ErrorCodes::BAD_ARGUMENTS,
                    "File URI '{}' refers to host '{}', but `file://` paths are always read from the local "
                    "filesystem. Only an empty authority (`file:///path`) or `localhost` is supported",
                    path,
                    remote_authority);
        }

        check_local_path_inside_user_files(target_decomposed.key);
    }

    auto check_read_access = [&]
    {
        const auto source = sourceForScheme(target_scheme_normalized);
        if (!source)
        {
            if (read_check)
            {
                read_check->openable = false;
                return;
            }
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported storage scheme '{}' in path '{}'", target_scheme_normalized, path);
        }

        const std::string & uri_to_check = target_scheme_normalized == "file" ? target_decomposed.key : path;
        if (read_check)
            read_check->granted = context->getAccess()->isGrantedWithFilter(
                DB::AccessType::READ, AccessTypeObjects::toStringSource(*source), normalizeAccessURI(uri_to_check));
        else
            context->getAccess()->checkAccessWithFilter(
                DB::AccessType::READ, AccessTypeObjects::toStringSource(*source), normalizeAccessURI(uri_to_check));
    };

    #if USE_AWS_S3
    if (target_scheme_normalized == "s3" || target_scheme_normalized == "https" || target_scheme_normalized == "http")
    {
        auto s3_uri = parseMetadataS3URI(path, target_decomposed);

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
                && keyIsInsidePrefix(base_s3_uri.key, key_to_use))
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
                    check_read_access();
                    if (read_check != nullptr)
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

        checkRemoteHostIsAllowed(context, endpoint_to_use);
        check_read_access();
        if (read_check != nullptr)
            return std::nullopt;

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
                }
            }
        }

        std::string storage_cache_key = "s3://" + s3_uri.bucket + "@" + endpoint_to_use
            + "#propagate=" + (propagate_creds ? "1" : "0")
            + "#restrict=" + (restricts_server_credentials ? "1" : "0");

        /// Credential rotation makes older cache entries unreachable; evict them by their common prefix.
        std::string supersedes_prefix;
        if (!access_key_id.empty() || !session_token.empty())
        {
            supersedes_prefix = storage_cache_key + "#cred=";

            SipHash creds_hash;
            creds_hash.update(access_key_id);
            creds_hash.update(secret_access_key);
            creds_hash.update(session_token);
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
                if (!access_key_id.empty())
                    s3_settings.auth_settings[S3AuthSetting::access_key_id] = access_key_id;
                if (!secret_access_key.empty())
                    s3_settings.auth_settings[S3AuthSetting::secret_access_key] = secret_access_key;
                if (!session_token.empty())
                    s3_settings.auth_settings[S3AuthSetting::session_token] = session_token;
                if (!region.empty())
                    s3_settings.auth_settings[S3AuthSetting::region] = region;

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

                const bool inside_table_prefix = keyIsInsidePrefix(table_location_decomposed.key, target_decomposed.key);

                if (base_endpoint == target_endpoint && inside_table_prefix)
                    use_base_storage = true;

                if (!use_base_storage && base_scheme_normalized == "hdfs" && inside_table_prefix)
                {
                    if (table_location_decomposed.authority == target_decomposed.authority)
                        use_base_storage = true;
                }
            }
        }

        if (use_base_storage)
            return std::make_pair(base_storage, target_decomposed.key);
    }
    #endif

    if (base_scheme_normalized == target_scheme_normalized && table_location_decomposed.authority == target_decomposed.authority)
    {
        const bool inside_table_location = keyIsInsidePrefix(table_location_decomposed.key, target_decomposed.key);

        /// Prefix-scoped backends accept full keys; other backends need paths inside `location` re-rooted.
        if (isPrefixScopedScheme(target_scheme_normalized))
        {
            if (inside_table_location)
                return std::make_pair(base_storage, target_decomposed.key);
        }
        else if (inside_table_location)
            return std::nullopt;
        else
        {
            check_read_access();
            if (read_check != nullptr)
                return std::nullopt;
            return std::make_pair(base_storage, target_decomposed.key);
        }
    }

    const std::string type_for_factory = factoryTypeForScheme(target_scheme_normalized);
    if (type_for_factory.empty())
    {
        if (read_check)
        {
            read_check->openable = false;
            return std::nullopt;
        }
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported storage scheme '{}' in path '{}'", target_scheme_normalized, path);
    }

    /// Each local storage is rooted at one parent directory, so the directory must be part of its cache key.
    std::string file_dir_path;
    std::string cache_key;
    if (target_scheme_normalized == "file")
    {
        std::filesystem::path fs_path(target_decomposed.key);
        file_dir_path = fs_path.parent_path().string();
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

            const std::string target_account = Poco::toLower(account_name);
            const std::string base_account = azureAccountFromServiceUrl(conn_params.getConnectionURL());

            if (!target_account.empty() && !base_account.empty() && target_account != base_account)
                throw DB::Exception(
                    DB::ErrorCodes::BAD_ARGUMENTS,
                    "Iceberg metadata references Azure storage account '{}', which differs from the table's "
                    "base account '{}'. Reading across Azure accounts is not supported; configure access to "
                    "account '{}'.",
                    account_name, base_account, account_name);

            checkRemoteHostIsAllowed(context, conn_params.getConnectionURL());
            check_read_access();
            if (read_check != nullptr)
                return std::nullopt;

            /// Clone connection parameters to preserve SAS and workload identity authentication, which config cannot express.
            return getOrCreateStorage(
                cache_key,
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
                });
        }
    }
#endif

    if (!target_decomposed.authority.empty())
        checkRemoteHostIsAllowed(context, target_scheme_normalized + "://" + target_decomposed.authority + "/");

    check_read_access();
    if (read_check != nullptr)
        return std::nullopt;

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

                cfg.setString(config_prefix + ".container_name", container_name);
                if (!account_name.empty())
                    cfg.setString(config_prefix + ".account_name", account_name);
            }
            else if (target_scheme_normalized == "hdfs")
            {
                auto endpoint = target_scheme_normalized + "://" + target_decomposed.authority;
                if (!endpoint.empty() && endpoint.back() != '/')
                    endpoint.push_back('/');
                cfg.setString(config_prefix + ".endpoint", endpoint);
            }
        });
}

std::optional<std::pair<DB::ObjectStoragePtr, std::string>> tryResolveObjectStorageForPath(
    const std::string & table_location,
    const std::string & path,
    const DB::ObjectStoragePtr & base_storage,
    ExternalStorageCache & external_storages,
    const DB::ContextPtr & context)
{
    return tryResolveObjectStorageForPathImpl(table_location, path, base_storage, external_storages, context, /*read_check*/ nullptr);
}

static PathReadCheck checkPathRead(
    const std::string & table_location,
    const std::string & path,
    const DB::ObjectStoragePtr & base_storage,
    const DB::ContextPtr & context)
{
    ExternalStorageCache unused_storages;
    PathReadCheck read_check;
    tryResolveObjectStorageForPathImpl(table_location, path, base_storage, unused_storages, context, &read_check);
    return read_check;
}

bool isPathReadGranted(
    const std::string & table_location,
    const std::string & path,
    const DB::ObjectStoragePtr & base_storage,
    const DB::ContextPtr & context)
{
    return checkPathRead(table_location, path, base_storage, context).granted;
}

bool isPathReadable(
    const std::string & table_location,
    const std::string & path,
    const DB::ObjectStoragePtr & base_storage,
    const DB::ContextPtr & context)
{
    const auto read_check = checkPathRead(table_location, path, base_storage, context);
    return read_check.granted && read_check.openable;
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
