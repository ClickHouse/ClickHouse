#include <Databases/Iceberg/ICatalog.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Poco/String.h>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

namespace Iceberg
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

    auto storage_type_str = location.substr(0, pos);
    auto storage_type = magic_enum::enum_cast<StorageType>(Poco::toUpper(storage_type_str));

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

    LOG_TEST(getLogger("TableMetadata"),
             "Parsed location without path: {}, path: {}",
             location_without_path, path);
}

std::string TableMetadata::getLocation(bool path_only) const
{
    if (!with_location)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Data location was not requested");

    if (path_only)
        return path;
    else
        return std::filesystem::path(location_without_path) / path;
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

StorageType TableMetadata::getStorageType() const
{
    return parseStorageTypeFromLocation(location_without_path);
}

}
