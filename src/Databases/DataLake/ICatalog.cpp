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

    auto capitalize_first_letter = [] (const std::string & s)
    {
        auto result = Poco::toLower(s);
        result[0] = std::toupper(result[0]);
        return result;
    };

    auto pos = location.find("://");
    if (pos == std::string::npos)
    {
        throw DB::Exception(
            DB::ErrorCodes::NOT_IMPLEMENTED,
            "Unexpected path format: {}", location);
    }

    auto storage_type_str = location.substr(0, pos);
    if (capitalize_first_letter(storage_type_str) == "File")
        storage_type_str = "Local";

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
    else
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

}
