#include "StorageSystemLicenses.h"

#include <algorithm>
#include <DataTypes/DataTypeString.h>


extern const char * library_licenses[];

namespace DB
{
NamesAndTypesList StorageSystemLicenses::getNamesAndTypes()
{
    return {
        {"library_name", std::make_shared<DataTypeString>()},
        {"license_type", std::make_shared<DataTypeString>()},
        {"license_path", std::make_shared<DataTypeString>()},
        {"license_text", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemLicenses::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    for (const auto * it = library_licenses; *it; it += 4)
    {
        res_columns[0]->insert(String(it[0]));
        res_columns[1]->insert(String(it[1]));
        res_columns[2]->insert(String(it[2]));
        res_columns[3]->insert(String(it[3]));
    }
}
}
