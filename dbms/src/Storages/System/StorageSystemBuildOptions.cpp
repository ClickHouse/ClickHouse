#include "StorageSystemBuildOptions.h"

#include <DataTypes/DataTypeString.h>
#include <Interpreters/Settings.h>

extern const char * auto_config_build[];

namespace DB
{

NamesAndTypesList StorageSystemBuildOptions::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemBuildOptions::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    for (auto it = auto_config_build; *it; it += 2)
    {
        res_columns[0]->insert(String(it[0]));
        res_columns[1]->insert(String(it[1]));
    }
}

}
