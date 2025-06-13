#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/DatabaseFactory.h>
#include <Storages/System/StorageSystemDatabaseEngines.h>

namespace DB
{

ColumnsDescription StorageSystemDatabaseEngines::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "The name of database engine."},
    };
}

void StorageSystemDatabaseEngines::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (const auto & [engine, _] : DatabaseFactory::instance().getDatabaseEngines())
    {
        int i = 0;
        res_columns[i++]->insert(engine);
    }
}

}
