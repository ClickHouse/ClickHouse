#include "DiskType.h"

namespace DB
{

bool DataSourceDescription::operator==(const DataSourceDescription & other) const
{
    return std::tie(type, object_storage_type, description, is_encrypted) == std::tie(other.type, other.object_storage_type, other.description, other.is_encrypted);
}

bool DataSourceDescription::sameKind(const DataSourceDescription & other) const
{
    return std::tie(type, object_storage_type, description)
        == std::tie(other.type, other.object_storage_type, other.description);
}

}
