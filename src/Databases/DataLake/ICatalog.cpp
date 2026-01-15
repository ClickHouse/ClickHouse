#include <Databases/DataLake/ICatalog.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Poco/String.h>

#include <filesystem>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
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
            result[0] = std::toupper(result[0]);
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
    else if (capitalize_first_letter(storage_type_str) == "S3a")
        storage_type_str = "S3";
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
    bucket = location_.substr(pos_to_bucket, pos_to_path - pos_to_bucket);

    LOG_TEST(getLogger("TableMetadata"),
             "Parsed location without path: {}, path: {}",
             location_without_path, path);
}

std::string TableMetadata::getLocation() const
{
    if (!with_location)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Data location was not requested");

    if (!endpoint.empty())
        return constructLocation(endpoint);

    return std::filesystem::path(location_without_path) / path;
}

std::string TableMetadata::getLocationWithEndpoint(const std::string & endpoint_) const
{
    if (!with_location)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Data location was not requested");

    if (endpoint_.empty())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Passed endpoint is empty");

    return constructLocation(endpoint_);
}

std::string TableMetadata::constructLocation(const std::string & endpoint_) const
{
    std::string location = endpoint_;
    if (location.ends_with('/'))
        location.pop_back();

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
        else if (!endpoint.empty() && data_location.starts_with(endpoint))
            data_location = data_location.substr(endpoint.size());

        if (metadata_location.starts_with(data_location))
        {
            size_t remove_slash = metadata_location[data_location.size()] == '/' ? 1 : 0;
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
