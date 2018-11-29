#include <DataTypes/DataTypeString.h>
#include <Storages/StorageFactory.h>
#include <Storages/System/StorageSystemTableEngines.h>

namespace DB
{

NamesAndTypesList StorageSystemTableEngines::getNamesAndTypes()
{
    return {{"name", std::make_shared<DataTypeString>()}};
}

void StorageSystemTableEngines::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    const auto & storages = StorageFactory::instance().getAllStorages();
    for (const auto & pair : storages)
    {
        res_columns[0]->insert(pair.first);
    }
}

}
