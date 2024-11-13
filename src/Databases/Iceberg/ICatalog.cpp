#include <Databases/Iceberg/ICatalog.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

namespace Iceberg
{

std::string ICatalog::TableMetadata::getPath() const
{
    if (!with_location)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Data location was not requested");

    if (location.starts_with("s3://"))
        return location.substr(std::strlen("s3://"));
    else
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Unexpected path format: {}", location);
}

const DB::NamesAndTypesList & ICatalog::TableMetadata::getSchema() const
{
    if (!with_schema)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Data location was not requested");
    return schema;
}

}
