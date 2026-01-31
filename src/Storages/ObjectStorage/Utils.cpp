#include <Core/Settings.h>
#include <Common/filesystemHelpers.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Disks/IO/getThreadPoolReader.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Poco/UUIDGenerator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageFactory.h>
#include <Poco/Util/MapConfiguration.h>
#include <IO/S3/URI.h>
#include <filesystem>
#include <functional>
#if USE_AWS_S3
#include <Disks/DiskObjectStorage/ObjectStorages/S3/S3ObjectStorage.h>
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
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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

std::string factoryTypeForScheme(const std::string & normalized_scheme)
{
    if (normalized_scheme == "s3") return "s3";
    if (normalized_scheme == "abfs") return "azure";
    if (normalized_scheme == "hdfs") return "hdfs";
    if (normalized_scheme == "file") return "local";
    return "";
}

#if USE_AWS_S3
/// For s3:// URIs (generic), bucket needs to match.
/// For explicit http(s):// URIs, both bucket and endpoint must match.
bool s3URIMatches(const S3::URI & target_uri, const std::string & base_bucket, const std::string & base_endpoint, const std::string & target_scheme_normalized)
{
    bool bucket_matches = (target_uri.bucket == base_bucket);
    bool endpoint_matches = (target_uri.endpoint == base_endpoint);
    bool is_generic_s3_uri = (target_scheme_normalized == "s3");
    return bucket_matches && (endpoint_matches || is_generic_s3_uri);
}
#endif
std::pair<ObjectStoragePtr, std::string> getOrCreateStorageAndKey(
    const std::string & cache_key,
    const std::string & key_to_use,
    const std::string & storage_type,
    SecondaryStorages & secondary_storages,
    const ContextPtr & context,
    std::function<void(Poco::Util::MapConfiguration &, const std::string &)> configure_fn)
{
    {
        std::lock_guard lock(secondary_storages.mutex);
        if (auto it = secondary_storages.storages.find(cache_key); it != secondary_storages.storages.end())
            return {it->second, key_to_use};
    }

    Poco::AutoPtr<Poco::Util::MapConfiguration> cfg(new Poco::Util::MapConfiguration);
    const std::string config_prefix = "object_storages." + cache_key;

    cfg->setString(config_prefix + ".object_storage_type", storage_type);

    configure_fn(*cfg, config_prefix);

    ObjectStoragePtr storage = ObjectStorageFactory::instance().create(cache_key, *cfg, config_prefix, context, /*skip_access_check*/ true);

    {
        std::lock_guard lock(secondary_storages.mutex);
        auto [it, inserted] = secondary_storages.storages.emplace(cache_key, storage);
        if (!inserted)
            return {it->second, key_to_use};
    }

    return {storage, key_to_use};
}
#endif

bool isAbsolutePath(const std::string & path)
{
    if (!path.empty() && (path.front() == '/' || path.find("://") != std::string_view::npos))
        return true;

    return false;
}

/// Normalize a path string by removing redundant components and leading slashes.
std::string normalizePathString(const std::string & path)
{
    std::filesystem::path fs_path(path);
    std::filesystem::path normalized = fs_path.lexically_normal();

    std::string normalized_result = normalized.string();

    while (!normalized_result.empty() && normalized_result.front() == '/')
        normalized_result = normalized_result.substr(1);

    return normalized_result;
}

/// Convert a path (relative to table location ot absolute path) to a key that will be looked up in the object storage.
///
/// - If `table_location` is empty, the path is treated as already relative to storage root.
/// - If `path` is an absolute path, its key component (without scheme/authority) is returned.
/// - If `table_location` parses to a URI whose key part is empty, `path` is returned unchanged (exception will be thrown when looking up non-existing object in storage)
///
/// - Otherwise, `path` is treated as relative to `table_location`'s key:
/// leading '/' stripped, concatenated to table_location key, and the result is normalized.
std::string convertPathToKeyInStorage(const std::string & table_location, const std::string & path)
{
    if (table_location.empty())
    {
        if (!path.empty() && path.front() == '/')
            return path.substr(1);
        return path;
    }

    if (isAbsolutePath(path))
        return SchemeAuthorityKey(path).key; // Absolute path, return the key part

    SchemeAuthorityKey base{table_location};
    if (base.key.empty())
        return path; // Table location is empty, return the path as is

    std::string base_key_trimmed = base.key;
    while (!base_key_trimmed.empty() && base_key_trimmed.front() == '/')
        base_key_trimmed = base_key_trimmed.substr(1);
    while (!base_key_trimmed.empty() && base_key_trimmed.back() == '/')
        base_key_trimmed.pop_back();

    std::string rel_path = path;
    while (!rel_path.empty() && rel_path.front() == '/')
        rel_path = rel_path.substr(1);

    if (!base_key_trimmed.empty() && (rel_path == base_key_trimmed || rel_path.starts_with(base_key_trimmed + "/")))
        return normalizePathString(rel_path);  // Path already includes table location

    std::string result = base.key;
    if (!result.empty() && result.back() != '/')
        result += '/';
    result += rel_path;

    return normalizePathString(result);
}

}

SchemeAuthorityKey::SchemeAuthorityKey(const std::string & uri)
{
    if (uri.empty())
        return;

    if (auto scheme_sep = uri.find("://"); scheme_sep != std::string_view::npos)
    {
        scheme = Poco::toLower(uri.substr(0, scheme_sep));
        auto rest = uri.substr(scheme_sep + 3); // skip ://

        // authority is up to next '/'
        auto slash = rest.find('/');
        if (slash == std::string_view::npos)
        {
            /// Bad URI: missing path component after authority.
            /// Exception will be thrown when looking up non-existing object in the storage, so we can just return here.
            authority = std::string(rest);
            key = "/";
            return;
        }
        authority = std::string(rest.substr(0, slash));
        /// For file:// URIs, the path is absolute, so we need to keep the leading '/'
        /// e.g. file:///home/user/data -> scheme="file", authority="", key="/home/user/data"
        if (scheme == "file")
            key = std::string(rest.substr(slash));
        else
            key = std::string(rest.substr(++slash));
        return;
    }

    /// Check for scheme:/path (common for file: https://datatracker.ietf.org/doc/html/rfc8089#appendix-B)
    if (auto colon = uri.find(':'); colon != std::string_view::npos && colon > 0)
    {
        auto after_colon = uri.substr(colon + 1);

        if (!after_colon.empty() && after_colon[0] == '/')
        {
            scheme = Poco::toLower(uri.substr(0, colon));
            authority = "";  // No authority
            key = std::string(after_colon);
            return;
        }
    }

    // Relative path (paths starting with '/' without a scheme are now handled by the caller)
    key = std::string(uri);
}

std::optional<String> checkAndGetNewFileOnInsertIfNeeded(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const StorageObjectStorageQuerySettings & settings,
    const String & key,
    size_t sequence_number)
{
    if (settings.truncate_on_insert
        || !object_storage.exists(StoredObject(key)))
        return std::nullopt;

    if (settings.create_new_file_on_insert)
    {
        auto pos = key.find_first_of('.');
        String new_key;
        do
        {
            new_key = key.substr(0, pos) + "." + std::to_string(sequence_number) + (pos == std::string::npos ? "" : key.substr(pos));
            ++sequence_number;
        }
        while (object_storage.exists(StoredObject(new_key)));

        return new_key;
    }

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Object in bucket {} with key {} already exists. "
        "If you want to overwrite it, enable setting {}_truncate_on_insert, if you "
        "want to create a new file on each insert, enable setting {}_create_new_file_on_insert",
        configuration.getNamespace(), key, configuration.getTypeName(), configuration.getTypeName());
}

void resolveSchemaAndFormat(
    ColumnsDescription & columns,
    std::string & format,
    ObjectStoragePtr object_storage,
    const StorageObjectStorageConfigurationPtr & configuration,
    std::optional<FormatSettings> format_settings,
    std::string & sample_path,
    const ContextPtr & context)
{
    if (format == "auto")
    {
        if (configuration->isDataLakeConfiguration())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Format must be already specified for {} storage.",
                configuration->getTypeName());
        }
    }

    if (columns.empty())
    {
        if (configuration->isDataLakeConfiguration())
        {
            auto table_structure = configuration->tryGetTableStructureFromMetadata(context);
            if (table_structure)
                columns = table_structure.value();
        }

        if (columns.empty())
        {
            if (format == "auto")
            {
                std::tie(columns, format) = StorageObjectStorage::resolveSchemaAndFormatFromData(
                    object_storage, configuration, format_settings, sample_path, context);
            }
            else
            {
                chassert(!format.empty());
                columns = StorageObjectStorage::resolveSchemaFromData(object_storage, configuration, format_settings, sample_path, context);
            }
        }
    }
    else if (format == "auto")
    {
        format = StorageObjectStorage::resolveFormatFromData(object_storage, configuration, format_settings, sample_path, context);
    }

    validateSupportedColumns(columns, *configuration);
}

void validateSupportedColumns(
    ColumnsDescription & columns,
    const StorageObjectStorageConfiguration & configuration)
{
    if (!columns.hasOnlyOrdinary())
    {
        /// We don't allow special columns.
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Special columns like MATERIALIZED, ALIAS or EPHEMERAL are not supported for {} storage.",
            configuration.getTypeName());
    }
}

ASTs::iterator getFirstKeyValueArgument(ASTs & args)
{
    ASTs::iterator first_key_value_arg_it = args.end();
    for (auto it = args.begin(); it != args.end(); ++it)
    {
        const auto * function_ast = (*it)->as<ASTFunction>();
        if (function_ast && function_ast->name == "equals")
        {
             if (first_key_value_arg_it == args.end())
                first_key_value_arg_it = it;
        }
        else if (first_key_value_arg_it != args.end())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Expected positional arguments to go before key-value arguments");
        }
    }
    return first_key_value_arg_it;
}

std::unordered_map<std::string, Field> parseKeyValueArguments(const ASTs & function_args, ContextPtr context)
{
    std::unordered_map<std::string, Field> key_value_args;
    for (const auto & arg : function_args)
    {
        const auto * function_ast = arg->as<ASTFunction>();
        if (!function_ast || function_ast->name != "equals")
            continue;

        auto * args_expr = assert_cast<ASTExpressionList *>(function_ast->arguments.get());
        auto & children = args_expr->children;
        if (children.size() != 2)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Key value argument is incorrect: expected 2 arguments, got {}",
                children.size());
        }

        auto key_literal = evaluateConstantExpressionOrIdentifierAsLiteral(children[0], context);
        auto value_literal = evaluateConstantExpressionOrIdentifierAsLiteral(children[1], context);

        auto arg_name_value = key_literal->as<ASTLiteral>()->value;
        if (arg_name_value.getType() != Field::Types::Which::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected string as credential name");

        auto arg_name = arg_name_value.safeGet<String>();
        auto arg_value = value_literal->as<ASTLiteral>()->value;

        auto inserted = key_value_args.emplace(arg_name, arg_value).second;
        if (!inserted)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate key value argument: {}", arg_name);
    }
    return key_value_args;
}

ParseFromDiskResult parseFromDisk(ASTs args, bool with_structure, ContextPtr context, const fs::path & prefix)
{
    if (args.size() > 3 + with_structure)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Storage requires {} arguments maximum.",
            2 + with_structure);

    std::unordered_map<std::string_view, size_t> engine_args_to_idx;

    if (with_structure)
    {
        if (args.size() > 3)
            engine_args_to_idx = {{"path", 0}, {"format", 1}, {"structure", 2}, {"compression_method", 3}};
        else if (args.size() > 2)
            engine_args_to_idx = {{"path", 0}, {"format", 1}, {"structure", 2}};
        else if (args.size() > 1)
            engine_args_to_idx = {{"path", 0}, {"structure", 1}};
        else if (!args.empty())
            engine_args_to_idx = {{"path", 0}};
    }
    else if (args.size() > 2)
        engine_args_to_idx = {{"path", 0}, {"format", 1}, {"compression_method", 2}};
    else if (args.size() > 1)
        engine_args_to_idx = {{"path", 0}, {"format", 1}};
    else if (!args.empty())
        engine_args_to_idx = {{"path", 0}};

    ASTs key_value_asts;
    if (auto first_key_value_arg_it = getFirstKeyValueArgument(args);
        first_key_value_arg_it != args.end())
    {
        key_value_asts = ASTs(first_key_value_arg_it, args.end());
    }

    auto key_value_args = parseKeyValueArguments(key_value_asts, context);

    ParseFromDiskResult result;
    if (auto path_value = getFromPositionOrKeyValue<String>("path", args, engine_args_to_idx, key_value_args);
        path_value.has_value())
    {
        result.path_suffix = path_value.value();
        if (result.path_suffix.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty path is not allowed");

        if (result.path_suffix.starts_with('/') || !pathStartsWith(prefix / fs::path(result.path_suffix), prefix))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path suffixes starting with '.' or '..' and absolute paths are not allowed. Please specify relative path");
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path should be specified as first argument or via `path = <path/to/data>` key value argument");

    if (auto format_value = getFromPositionOrKeyValue<String>("format", args, engine_args_to_idx, key_value_args);
        format_value.has_value())
    {
        result.format = format_value.value();
    }

    if (auto structure_value = getFromPositionOrKeyValue<String>("structure", args, engine_args_to_idx, key_value_args);
        structure_value.has_value())
    {
        result.structure = structure_value.value();
    }

    if (auto compression_method_value = getFromPositionOrKeyValue<String>("compression_method", args, engine_args_to_idx, key_value_args);
        compression_method_value.has_value())
    {
        result.compression_method = compression_method_value.value();
    }
    return result;
}

namespace Setting
{
extern const SettingsUInt64 max_download_buffer_size;
extern const SettingsBool use_cache_for_count_from_files;
extern const SettingsString filesystem_cache_name;
extern const SettingsUInt64 filesystem_cache_boundary_alignment;
}

std::string makeAbsolutePath(const std::string & table_location, const std::string & path)
{
    if (isAbsolutePath(path))
        return path;

    auto table_location_decomposed = SchemeAuthorityKey(table_location);

    std::string normalized_key = convertPathToKeyInStorage(table_location, path);

    if (!table_location_decomposed.scheme.empty())
        return table_location_decomposed.scheme + "://" + table_location_decomposed.authority + "/" + normalized_key;

    return normalized_key;
}

#if USE_AVRO
std::pair<DB::ObjectStoragePtr, std::string> resolveObjectStorageForPath(
    const std::string & table_location,
    const std::string & path,
    const DB::ObjectStoragePtr & base_storage,
    SecondaryStorages & secondary_storages,
    const DB::ContextPtr & context)
{
    if (!isAbsolutePath(path))
        return {base_storage, convertPathToKeyInStorage(table_location, path)}; // Relative path definitely goes to base storage

    SchemeAuthorityKey table_location_decomposed{table_location};
    SchemeAuthorityKey target_decomposed{path};

    if (target_decomposed.scheme.empty() && target_decomposed.key.starts_with('/'))
        return {base_storage, convertPathToKeyInStorage(table_location, target_decomposed.key)};

    const std::string base_scheme_normalized = normalizeScheme(table_location_decomposed.scheme);
    const std::string target_scheme_normalized = normalizeScheme(target_decomposed.scheme);

    // For S3 URIs, use S3::URI to properly handle all kinds of URIs, e.g. https://s3.amazonaws.com/bucket/... == s3://bucket/...
    #if USE_AWS_S3
    if (target_scheme_normalized == "s3" || target_scheme_normalized == "https" || target_scheme_normalized == "http")
    {
        std::string normalized_path = path;
        if (target_decomposed.scheme == "s3a" || target_decomposed.scheme == "s3n")
        {
            normalized_path = "s3://" + target_decomposed.authority + "/" + target_decomposed.key;
        }
        else if (target_decomposed.scheme == "gcs")
        {
            normalized_path = "gs://" + target_decomposed.authority + "/" + target_decomposed.key;
        }
        S3::URI s3_uri(normalized_path);

        std::string key_to_use = s3_uri.key;

        bool use_base_storage = false;
        if (base_storage->getType() == ObjectStorageType::S3)
        {
            if (auto s3_storage = std::dynamic_pointer_cast<S3ObjectStorage>(base_storage))
            {
                const std::string base_bucket = s3_storage->getObjectsNamespace();
                const std::string base_endpoint = s3_storage->getDescription();

                if (s3URIMatches(s3_uri, base_bucket, base_endpoint, target_scheme_normalized))
                    use_base_storage = true;
            }
        }

        if (!use_base_storage && (base_scheme_normalized == "s3" || base_scheme_normalized == "https" || base_scheme_normalized == "http"))
        {
            std::string normalized_table_location = table_location;
            if (table_location_decomposed.scheme == "s3a" || table_location_decomposed.scheme == "s3n")
            {
                normalized_table_location = "s3://" + table_location_decomposed.authority + "/" + table_location_decomposed.key;
            }
            else if (table_location_decomposed.scheme == "gcs")
            {
                normalized_table_location = "gs://" + table_location_decomposed.authority + "/" + table_location_decomposed.key;
            }
            S3::URI base_s3_uri(normalized_table_location);

            if (s3URIMatches(s3_uri, base_s3_uri.bucket, base_s3_uri.endpoint, target_scheme_normalized))
                use_base_storage = true;
        }

        if (use_base_storage)
            return {base_storage, key_to_use};

        const std::string storage_cache_key = "s3://" + s3_uri.bucket + "@" + (s3_uri.endpoint.empty() ? "amazonaws.com" : s3_uri.endpoint);

        return getOrCreateStorageAndKey(
            storage_cache_key,
            key_to_use,
            "s3",
            secondary_storages,
            context,
            [&](Poco::Util::MapConfiguration & cfg, const std::string & config_prefix)
            {
                bool endpoint_explicit = (target_decomposed.scheme == "http" || target_decomposed.scheme == "https");

                std::string endpoint_to_use;

                if (endpoint_explicit)
                {
                    endpoint_to_use = s3_uri.endpoint.empty()
                        ? ("https://" + s3_uri.bucket + ".s3.amazonaws.com")
                        : s3_uri.endpoint;
                }
                else
                {
                    std::string base_endpoint;
                    if (base_storage->getType() == ObjectStorageType::S3)
                            base_endpoint = base_storage->getDescription();

                    if (!base_endpoint.empty())
                    {
                        if (base_endpoint.find(".s3.") != std::string::npos && base_endpoint.find(".amazonaws.com") != std::string::npos)
                        {
                            /// AWS-style: https://oldbucket.s3.us-east-1.amazonaws.com -> https://newbucket.s3.us-east-1.amazonaws.com
                            size_t s3_pos = base_endpoint.find(".s3.");
                            size_t scheme_end = base_endpoint.find("://");
                            if (scheme_end != std::string::npos)
                            {
                                std::string scheme = base_endpoint.substr(0, scheme_end + 3);
                                std::string suffix = base_endpoint.substr(s3_pos);

                                /// Trim path after endpoint
                                size_t slash_pos = suffix.find('/', 1);
                                if (slash_pos != std::string::npos)
                                    suffix = suffix.substr(0, slash_pos);
                                endpoint_to_use = scheme + s3_uri.bucket + suffix;
                            }
                        }
                        else
                        {
                            /// Path-style (e.g. minio): http://host:port/oldbucket -> http://host:port/newbucket
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
                        }
                    }

                    /// Fallback: base storage is not S3
                    if (endpoint_to_use.empty())
                    {
                        endpoint_to_use = s3_uri.endpoint.empty()
                            ? ("https://" + s3_uri.bucket + ".s3.amazonaws.com")
                            : s3_uri.endpoint;
                    }
                }

                cfg.setString(config_prefix + ".endpoint", endpoint_to_use);

                /// Copy credentials from base storage if it's also S3
                if (base_storage->getType() == ObjectStorageType::S3)
                {
                    if (auto s3_storage = std::dynamic_pointer_cast<S3ObjectStorage>(base_storage))
                    {
                        if (auto s3_client = s3_storage->tryGetS3StorageClient())
                        {
                            const auto credentials = s3_client->getCredentials();
                            const String & access_key_id = credentials.GetAWSAccessKeyId();
                            const String & secret_access_key = credentials.GetAWSSecretKey();
                            const String & session_token = credentials.GetSessionToken();
                            const String & region = s3_client->getRegion();

                            if (!access_key_id.empty())
                                cfg.setString(config_prefix + ".access_key_id", access_key_id);
                            if (!secret_access_key.empty())
                                cfg.setString(config_prefix + ".secret_access_key", secret_access_key);
                            if (!session_token.empty())
                                cfg.setString(config_prefix + ".session_token", session_token);
                            if (!region.empty())
                                cfg.setString(config_prefix + ".region", region);
                        }
                    }
                }
            });
    }
    #endif

    #if USE_HDFS
    if (target_scheme_normalized == "hdfs")
    {
        bool use_base_storage = false;

        // Check if base_storage matches (only if it's HDFS)
        if (base_storage->getType() == ObjectStorageType::HDFS)
        {
            if (auto hdfs_storage = std::dynamic_pointer_cast<HDFSObjectStorage>(base_storage))
            {
                const std::string base_url = hdfs_storage->getDescription();
                // Extract endpoint from base URL (hdfs://namenode:port/path -> hdfs://namenode:port)
                std::string base_endpoint;
                if (auto pos = base_url.find('/', base_url.find("//") + 2); pos != std::string::npos)
                    base_endpoint = base_url.substr(0, pos);
                else
                    base_endpoint = base_url;

                // For HDFS, compare endpoints (namenode addresses)
                std::string target_endpoint = target_scheme_normalized + "://" + target_decomposed.authority;

                if (base_endpoint == target_endpoint)
                    use_base_storage = true;

                // Also check if table_location matches
                if (!use_base_storage && base_scheme_normalized == "hdfs")
                {
                    if (table_location_decomposed.authority == target_decomposed.authority)
                        use_base_storage = true;
                }
            }
        }

        if (use_base_storage)
            return {base_storage, target_decomposed.key};
    }
    #endif

    /// Fallback for schemes not handled above (e.g., abfs, file)
    if (base_scheme_normalized == target_scheme_normalized && table_location_decomposed.authority == target_decomposed.authority)
        return {base_storage, target_decomposed.key};

    const std::string cache_key = target_scheme_normalized + "://" + target_decomposed.authority;

    const std::string type_for_factory = factoryTypeForScheme(target_scheme_normalized);
    if (type_for_factory.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported storage scheme '{}' in path '{}'", target_scheme_normalized, path);

    /// Handle storage types that need new storage creation
    return getOrCreateStorageAndKey(
        cache_key,
        target_decomposed.key,
        type_for_factory,
        secondary_storages,
        context,
        [&](Poco::Util::MapConfiguration & cfg, const std::string & config_prefix)
        {
            if (target_scheme_normalized == "file")
            {
                std::filesystem::path fs_path(target_decomposed.key);
                std::filesystem::path parent = fs_path.parent_path();
                std::string dir_path = parent.string();

                if (dir_path.empty() || dir_path == "/")
                    dir_path = "/";
                else if (dir_path.back() != '/')
                    dir_path += '/';

                cfg.setString(config_prefix + ".path", dir_path);
            }
            else if (target_scheme_normalized == "abfs")
            {
                std::string container_name;
                std::string account_name;
                const auto & authority = target_decomposed.authority;

                auto at_pos = authority.find('@');
                if (at_pos != std::string::npos)
                {
                    container_name = authority.substr(0, at_pos);
                    account_name = authority.substr(at_pos + 1);
                    /// Remove .dfs.core.windows.net suffix if present
                    auto suffix_pos = account_name.find('.');
                    if (suffix_pos != std::string::npos)
                        account_name = account_name.substr(0, suffix_pos);
                }
                else
                    container_name = authority;

                cfg.setString(config_prefix + ".container_name", container_name);
                if (!account_name.empty())
                    cfg.setString(config_prefix + ".account_name", account_name);

#if USE_AZURE_BLOB_STORAGE
                /// Copy credentials from base Azure storage if available
                if (base_storage->getType() == ObjectStorageType::Azure)
                {
                    if (auto azure_storage = std::dynamic_pointer_cast<AzureObjectStorage>(base_storage))
                    {
                        const auto & conn_params = azure_storage->getConnectionParameters();
                        const auto & auth_method = azure_storage->getAzureBlobStorageAuthMethod();

                        if (std::holds_alternative<AzureBlobStorage::ConnectionString>(auth_method))
                        {
                            cfg.setString(config_prefix + ".connection_string",
                                std::get<AzureBlobStorage::ConnectionString>(auth_method).toUnderType());
                        }
                        else
                        {
                            const auto & endpoint = conn_params.endpoint;
                            if (!endpoint.storage_account_url.empty())
                                cfg.setString(config_prefix + ".storage_account_url", endpoint.storage_account_url);
                            if (account_name.empty() && !endpoint.account_name.empty())
                                cfg.setString(config_prefix + ".account_name", endpoint.account_name);
                        }
                    }
                }
#endif
            }
            else if (target_scheme_normalized == "hdfs")
            {
                // HDFS endpoint must end with '/'
                auto endpoint = target_scheme_normalized + "://" + target_decomposed.authority;
                if (!endpoint.empty() && endpoint.back() != '/')
                    endpoint.push_back('/');
                cfg.setString(config_prefix + ".endpoint", endpoint);
            }
        });
}

#endif

}
