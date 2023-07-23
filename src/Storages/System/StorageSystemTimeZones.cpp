#include "StorageSystemTimeZones.h"

#include <algorithm>
#include <DataTypes/DataTypeString.h>


struct TimeZone { const char * name; const unsigned char * data; size_t size; };
extern TimeZone auto_time_zones[];

namespace DB
{
NamesAndTypesList StorageSystemTimeZones::getNamesAndTypes()
{
    return {
        {"time_zone", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemTimeZones::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    for (auto * it = auto_time_zones; it->name != nullptr; ++it)
        res_columns[0]->insert(String(it->name));
}
}
