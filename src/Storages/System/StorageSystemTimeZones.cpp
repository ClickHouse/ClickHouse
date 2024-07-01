#include "StorageSystemTimeZones.h"

#include <algorithm>
#include <DataTypes/DataTypeString.h>


extern const char * auto_time_zones[];

namespace DB
{
ColumnsDescription StorageSystemTimeZones::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"time_zone", std::make_shared<DataTypeString>(), "List of supported time zones."},
    };
}

void StorageSystemTimeZones::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (auto * it = auto_time_zones; *it; ++it)
        res_columns[0]->insert(String(*it));
}
}
