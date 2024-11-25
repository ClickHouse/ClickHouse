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

}
