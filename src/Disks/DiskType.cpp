#include "DiskType.h"

namespace DB
{

bool DataSourceDescription::operator==(const DataSourceDescription & o) const
{
    return std::tie(type, description, is_encrypted) == std::tie(o.type, o.description, o.is_encrypted);
}

}
