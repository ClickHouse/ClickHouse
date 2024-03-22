#include "DiskType.h"

namespace DB
{

bool DataSourceDescription::operator==(const DataSourceDescription & other) const
{
    return std::tie(type, description, is_encrypted) == std::tie(other.type, other.description, other.is_encrypted);
}

bool DataSourceDescription::sameKind(const DataSourceDescription & other) const
{
    return std::tie(type, description) == std::tie(other.type, other.description);
}

}
