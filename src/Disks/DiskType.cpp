#include <Disks/DiskType.h>
#include <Poco/String.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int LOGICAL_ERROR;
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
    if (check_type == "keeper")
        return MetadataStorageType::Keeper;
    if (check_type == "memory")
        return MetadataStorageType::Memory;

    throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
                    "MetadataStorageFactory: unknown metadata storage type: {}", type);
}

bool DataSourceDescription::operator==(const DataSourceDescription & other) const
{
    return std::tie(type, object_storage_type, description, is_encrypted, zookeeper_name) == std::tie(other.type, other.object_storage_type, other.description, other.is_encrypted, other.zookeeper_name);
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
    String str;
    switch (type)
    {
        case DataSourceType::Local:
            str = "local";
            break;
        case DataSourceType::RAM:
            str = "memory";
            break;
        case DataSourceType::ObjectStorage:
        {
            switch (object_storage_type)
            {
                case ObjectStorageType::S3:
                    str = "s3";
                    break;
                case ObjectStorageType::HDFS:
                    str = "hdfs";
                    break;
                case ObjectStorageType::Azure:
                    str = "azure_blob_storage";
                    break;
                case ObjectStorageType::Local:
                    str = "local_blob_storage";
                    break;
                case ObjectStorageType::Web:
                    str = "web";
                    break;
                case ObjectStorageType::None:
                    str = "none";
                    break;
                case ObjectStorageType::Max:
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected object storage type: Max");
            }
        }
    }

    str += fmt::format(" (description = '{}', is_encrypted = {}, is_cached = {}, zookeeper_name = '{}')",
                       description, is_encrypted, is_cached, zookeeper_name);

    return str;
}
}
