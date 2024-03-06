#include "StorageSystemLicenses.h"

#include <algorithm>
#include <DataTypes/DataTypeString.h>


extern const char * library_licenses[];

namespace DB
{
ColumnsDescription StorageSystemLicenses::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"library_name", std::make_shared<DataTypeString>(), "Name of the library."},
        {"license_type", std::make_shared<DataTypeString>(), "License type — e.g. Apache, MIT."},
        {"license_path", std::make_shared<DataTypeString>(), "Path to the file with the license text."},
        {"license_text", std::make_shared<DataTypeString>(), "License text."},
    };
}

void StorageSystemLicenses::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
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
