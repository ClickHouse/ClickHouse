#include "StorageSystemBuildOptions.h"

#include <DataTypes/DataTypeString.h>
#include <Core/Settings.h>

extern const char * auto_config_build[];

namespace DB
{

ColumnsDescription StorageSystemBuildOptions::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of the build option."},
        {"value", std::make_shared<DataTypeString>(), "Value of the build option."},
    };
}

void StorageSystemBuildOptions::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (auto * it = auto_config_build; *it; it += 2)
    {
        res_columns[0]->insert(it[0]);
        res_columns[1]->insert(it[1]);
    }
}

}
