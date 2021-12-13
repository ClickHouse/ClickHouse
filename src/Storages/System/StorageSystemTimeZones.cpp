#include "StorageSystemTimeZones.h"

#include <algorithm>
#include <DataTypes/DataTypeString.h>


extern const char * auto_time_zones[];

namespace DB
{
NamesAndTypesList StorageSystemTimeZones::getNamesAndTypes()
{
    return {
        {"time_zone", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemTimeZones::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    for (auto * it = auto_time_zones; *it; ++it)
        res_columns[0]->insert(String(*it));
}
}
