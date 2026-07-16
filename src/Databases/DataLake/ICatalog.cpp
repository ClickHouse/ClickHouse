#include <Databases/DataLake/ICatalog.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Poco/String.h>

#include <filesystem>

#include <Common/FailPoint.h>
#include <Poco/URI.h>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace DB::FailPoints
{
    extern const char database_iceberg_gcs[];
}

namespace DataLake
{

StorageType parseStorageTypeFromLocation(const std::string & location)
{
    /// Table location in catalog metadata always starts with one of s3://, file://, etc.
    /// So just extract this part of the path and deduce storage type from it.

    auto pos = location.find("://");
    if (pos == std::string::npos)
    {
        throw DB::Exception(
            DB::ErrorCodes::NOT_IMPLEMENTED,
            "Unexpected path format: {}", location);
    }

    return parseStorageTypeFromString(location.substr(0, pos));
}

StorageType parseStorageTypeFromString(const std::string & type)
{
    auto capitalize_first_letter = [] (const std::string & s)
    {
        auto result = Poco::toLower(s);
        if (!result.empty())
            result[0] = static_cast<char>(std::toupper(result[0]));
        return result;
    };

    std::string storage_type_str = type;
    auto pos = storage_type_str.find("://");
    if (pos != std::string::npos)
    {
        // convert s3://, file://, etc. to s3, file etc.
        storage_type_str = storage_type_str.substr(0,pos);
    }
    if (capitalize_first_letter(storage_type_str) == "File")
        storage_type_str = "Local";
    else if (capitalize_first_letter(storage_type_str) == "S3a" || storage_type_str == "oss" || storage_type_str == "gs")
    {
        fiu_do_on(DB::FailPoints::database_iceberg_gcs,
        {
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Google cloud storage converts to S3");
        });
        storage_type_str = "S3";
    }
    else if (storage_type_str == "abfss") /// Azure Blob File System Secure
        storage_type_str = "Azure";

    auto storage_type = magic_enum::enum_cast<StorageType>(capitalize_first_letter(storage_type_str));

    if (!storage_type)
    {
        throw DB::Exception(
            DB::ErrorCodes::NOT_IMPLEMENTED,
            "Unsupported storage type: {}", storage_type_str);
    }

    return *storage_type;
}

void TableMetadata::setLocation(const std::string & location_)
{
    if (!with_location)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Data location was not requested");

    /// Location has format:
    /// s3://<bucket>/path/to/table/data.
    /// We want to split s3://<bucket> and path/to/table/data.

    /// For Azure ABFSS: abfss://<container>@<account>.dfs.core.windows.net/path/to/table/data
    /// We want to split the bucket/container and path.

    auto pos = location_.find("://");
    if (pos == std::string::npos)
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Unexpected location format: {}", location_);

    // pos+3 to account for ://
    storage_type_str = location_.substr(0, pos+3);
    auto pos_to_bucket = pos + std::strlen("://");
    auto pos_to_path = location_.substr(pos_to_bucket).find('/');

    if (pos_to_path == std::string::npos)
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Unexpected location format: {}", location_);

    pos_to_path = pos_to_bucket + pos_to_path;

    location_without_path = location_.substr(0, pos_to_path);
    path = location_.substr(pos_to_path + 1);

    /// For Azure ABFSS format: abfss://container@account.dfs.core.windows.net/path
    /// The bucket (container) is the part before '@', not the whole string before '/'
    String bucket_part = location_.substr(pos_to_bucket, pos_to_path - pos_to_bucket);
    auto at_pos = bucket_part.find('@');
    if (at_pos != std::string::npos)
    {
        /// Azure ABFSS format: extract container (before @) and account (after @)
        bucket = bucket_part.substr(0, at_pos);
        azure_account_with_suffix = bucket_part.substr(at_pos + 1);

        /// Some catalogs (e.g. Apache Polaris) follow the ADLS Gen2 filesystem convention
        /// of including the container name as the first segment of the path in abfss:// locations,
        /// e.g. abfss://container@account.dfs.core.windows.net/container/actual/path.
        /// We record this as a flag so that `constructLocation` and `getMetadataLocation` can
        /// strip the redundant prefix when needed, while `path` itself is left intact so that
        /// `getLocation` remains a round-trip of `setLocation`.
        if (polaris_style_abfss_paths && path.starts_with(bucket + "/"))
            abfss_has_container_path_prefix = true;

        LOG_TEST(getLogger("TableMetadata"),
                 "Parsed Azure location - container: {}, account: {}, path: {}",
                 bucket, azure_account_with_suffix, path);
    }
    else
    {
        /// Standard format (S3, GCS, etc.)
        bucket = bucket_part;
        LOG_TEST(getLogger("TableMetadata"),
                 "Parsed location without path: {}, path: {}",
                 location_without_path, path);
    }
}

std::string TableMetadata::getLocation() const
{
    if (!with_location)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Data location was not requested");

    if (!endpoint.empty())
        return constructLocation(endpoint, DB::S3UriStyle::AUTO);

    return std::filesystem::path(location_without_path) / path;
}

std::string TableMetadata::getLocationWithEndpoint(const std::string & endpoint_, DB::S3UriStyle uri_style) const
{
    if (!with_location)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Data location was not requested");

    if (endpoint_.empty())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Passed endpoint is empty");

    return constructLocation(endpoint_, uri_style);
}

std::string TableMetadata::constructLocation(const std::string & endpoint_, DB::S3UriStyle uri_style) const
{
    std::string location = endpoint_;
    if (location.ends_with('/'))
        location.pop_back();

    /// For Azure storage, the endpoint format is: https://<account>.dfs.core.windows.net
    /// We need to construct: https://<account>.dfs.core.windows.net/<container>/<path>
    /// The bucket variable contains the container name for Azure.
    if (!azure_account_with_suffix.empty())
    {
        /// Azure storage - endpoint should be https://<account>.dfs.core.windows.net
        /// Construct the full URL with container and path.
        /// When the path carries a Polaris-style redundant container prefix (e.g. "c/actual/path"
        /// for container "c"), strip it before prepending the container, so we don't double it.
        std::string_view effective_path = path;
        if (abfss_has_container_path_prefix && effective_path.starts_with(bucket + "/"))
            effective_path = effective_path.substr(bucket.size() + 1);
        if (location.ends_with(bucket))
            return std::filesystem::path(location) / effective_path / "";
        return std::filesystem::path(location) / bucket / effective_path / "";
    }

    if (uri_style == DB::S3UriStyle::VIRTUAL_HOSTED)
    {
        /// Virtual-hosted style: embed the bucket name in the hostname.
        /// Transform https://endpoint.com[:port] -> https://bucket.endpoint.com[:port]/path/
        /// If the endpoint hostname already starts with the bucket (already virtual-hosted), don't add it again.
        Poco::URI endpoint_uri(location);
        const std::string & host = endpoint_uri.getHost();
        if (!host.starts_with(bucket + "."))
            endpoint_uri.setHost(bucket + "." + host);
        std::string vhosted_base = endpoint_uri.toString();
        if (vhosted_base.ends_with('/'))
            vhosted_base.pop_back();
        return std::filesystem::path(vhosted_base) / path / "";
    }

    if (uri_style == DB::S3UriStyle::PATH)
    {
        Poco::URI endpoint_uri(location);
        if (endpoint_uri.getHost().starts_with(bucket + "."))
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Cannot use PATH addressing style with endpoint '{}': "
                "the hostname already embeds bucket '{}' in virtual-hosted form",
                endpoint_uri.getHost(), bucket);
    }

    if (location.ends_with(bucket))
        return std::filesystem::path(location) / path / "";
    return std::filesystem::path(location) / bucket / path / "";
}

void TableMetadata::setEndpoint(const std::string & endpoint_)
{
    endpoint = endpoint_;
}

void TableMetadata::setSchema(const DB::NamesAndTypesList & schema_)
{
    if (!with_schema)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Data schema was not requested");

    schema = schema_;
}

const DB::NamesAndTypesList & TableMetadata::getSchema() const
{
    if (!with_schema)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Data schema was not requested");

    return schema;
}

void TableMetadata::setStorageCredentials(std::shared_ptr<IStorageCredentials> credentials_)
{
    if (!with_storage_credentials)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Storage credentials were not requested");

    storage_credentials = std::move(credentials_);
}

std::shared_ptr<IStorageCredentials> TableMetadata::getStorageCredentials() const
{
    if (!with_storage_credentials)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Data schema was not requested");

    return storage_credentials;
}

void TableMetadata::setDataLakeSpecificProperties(std::optional<DataLakeSpecificProperties> && metadata)
{
    data_lake_specific_metadata = metadata;
}

std::optional<DataLakeSpecificProperties> TableMetadata::getDataLakeSpecificProperties() const
{
    return data_lake_specific_metadata;
}

StorageType TableMetadata::getStorageType() const
{
    if (!storage_type_str.empty())
    {
        return parseStorageTypeFromString(storage_type_str);
    }
    return parseStorageTypeFromLocation(location_without_path);
}

bool TableMetadata::hasLocation() const
{
    return !location_without_path.empty();
}
bool TableMetadata::hasSchema() const
{
    return !schema.empty();
}
bool TableMetadata::hasStorageCredentials() const
{
    return storage_credentials != nullptr;
}

std::string TableMetadata::getMetadataLocation(const std::string & iceberg_metadata_file_location) const
{
    std::string metadata_location = iceberg_metadata_file_location;
    if (!metadata_location.empty())
    {
        std::string data_location = getLocation();

        // Use the actual storage type prefix (e.g., s3://, file://, etc.)
        if (metadata_location.starts_with(storage_type_str))
            metadata_location = metadata_location.substr(storage_type_str.size());
        if (data_location.starts_with(storage_type_str))
            data_location = data_location.substr(storage_type_str.size());
        else if (!endpoint.empty())
        {
            std::string normalized_endpoint = endpoint;
            if (normalized_endpoint.ends_with('/'))
                normalized_endpoint.pop_back();
            if (data_location.starts_with(normalized_endpoint))
            {
                data_location = data_location.substr(normalized_endpoint.size());
                if (azure_account_with_suffix.empty() && !data_location.empty() && data_location.front() == '/')
                    data_location = data_location.substr(1);
            }
        }

        /// For Azure ABFSS locations we need to reconcile two different formats:
        ///   - metadata_location (from catalog): "container@account.host/path/..."
        ///   - data_location (with endpoint set): "/container/path/" (HTTPS path after endpoint stripped)
        /// When no endpoint is set both sides are in ABFSS authority form and compare directly.
        if (!azure_account_with_suffix.empty() && !bucket.empty())
        {
            /// The host part after stripping the ABFSS protocol is: bucket@azure_account_with_suffix/
            std::string azure_host_prefix = bucket + "@" + azure_account_with_suffix + "/";

            /// For Polaris-style paths: the container name is repeated as the first path segment
            /// (e.g. abfss://c@account/c/actual/path). Strip that redundant prefix from both sides
            /// before the comparison below so we identify the correct relative path.
            /// This runs for both with-endpoint and without-endpoint cases.
            if (abfss_has_container_path_prefix)
            {
                auto strip_container = [&](std::string & location_str)
                {
                    if (location_str.starts_with(azure_host_prefix))
                    {
                        std::string_view after_host = std::string_view(location_str).substr(azure_host_prefix.size());
                        if (after_host.starts_with(bucket + "/"))
                        {
                            location_str = std::string(azure_host_prefix) + std::string(after_host.substr(bucket.size() + 1));
                        }
                    }
                };
                strip_container(metadata_location);
                strip_container(data_location);
            }

            /// With endpoint: data_location is now in HTTPS path form ("/container/path/").
            /// Convert metadata_location from ABFSS authority form ("container@account.host/path")
            /// to the matching HTTPS path form ("/container/path") so the prefix comparison works.
            if (!endpoint.empty() && metadata_location.starts_with(azure_host_prefix))
                metadata_location = "/" + bucket + "/" + metadata_location.substr(azure_host_prefix.size());
        }

        if (metadata_location.starts_with(data_location))
        {
            size_t remove_slash = (metadata_location.size() > data_location.size() && metadata_location[data_location.size()] == '/') ? 1 : 0;
            metadata_location = metadata_location.substr(data_location.size() + remove_slash);
        }
    }
    return metadata_location;
}

DB::SettingsChanges CatalogSettings::allChanged() const
{
    DB::SettingsChanges changes;
    changes.emplace_back("storage_endpoint", storage_endpoint);
    changes.emplace_back("aws_access_key_id", aws_access_key_id);
    changes.emplace_back("aws_secret_access_key", aws_secret_access_key);
    changes.emplace_back("region", region);
    changes.emplace_back("aws_role_arn", aws_role_arn);
    changes.emplace_back("aws_role_session_name", aws_role_session_name);

    return changes;
}

void ICatalog::createTable(const String & /*namespace_name*/, const String & /*table_name*/, const String & /*new_metadata_path*/, Poco::JSON::Object::Ptr /*metadata_content*/) const
{
    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "createTable is not implemented");
}

bool ICatalog::updateMetadata(const String & /*namespace_name*/, const String & /*table_name*/, const String & /*new_metadata_path*/, Poco::JSON::Object::Ptr /*new_snapshot*/) const
{
    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "updateMetadata is not implemented");
}

void ICatalog::dropTable(const String & /*namespace_name*/, const String & /*table_name*/) const
{
    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "dropTable is not implemented");
}

}
