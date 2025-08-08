#include "DiskType.h"
#include <Poco/String.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

MetadataStorageType metadataTypeFromString(const String & type)
{
    auto check_type = Poco::toLower(type);
    if (check_type == "local")
        return MetadataStorageType::Local;
    if (check_type == "plain")
        return MetadataStorageType::Plain;
    if (check_type == "plain_rewritable")
        return MetadataStorageType::PlainRewritable;
    if (check_type == "web")
        return MetadataStorageType::StaticWeb;

    throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
                    "MetadataStorageFactory: unknown metadata storage type: {}", type);
}

bool DataSourceDescription::operator==(const DataSourceDescription & other) const
{
    return std::tie(type, object_storage_type, description, is_encrypted) == std::tie(other.type, other.object_storage_type, other.description, other.is_encrypted);
}

bool DataSourceDescription::sameKind(const DataSourceDescription & other) const
{
    std::string_view our_description = description;
    if (our_description.ends_with('/') && our_description.length() > 1)
        our_description = our_description.substr(0, our_description.length() - 1);

    std::string_view other_description = other.description;
    if (other_description.ends_with('/') && other_description.length() > 1)
        other_description = other_description.substr(0, other_description.length() - 1);

    return std::tie(type, object_storage_type, our_description)
        == std::tie(other.type, other.object_storage_type, other_description);
}

std::string DataSourceDescription::toString() const
{
    switch (type)
    {
        case DataSourceType::Local:
            return "local";
        case DataSourceType::RAM:
            return "memory";
        case DataSourceType::ObjectStorage:
        {
            switch (object_storage_type)
            {
                case ObjectStorageType::S3:
                    return "s3";
                case ObjectStorageType::HDFS:
                    return "hdfs";
                case ObjectStorageType::Azure:
                    return "azure_blob_storage";
                case ObjectStorageType::Local:
                    return "local_blob_storage";
                case ObjectStorageType::Web:
                    return "web";
                case ObjectStorageType::None:
                    return "none";
            }
        }
    }
}
}
