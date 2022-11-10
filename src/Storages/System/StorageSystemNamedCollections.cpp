#include "StorageSystemNamedCollections.h"
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Access/Common/AccessType.h>
#include <Access/Common/AccessFlags.h>
#include <Storages/NamedCollections.h>


namespace DB
{

NamesAndTypesList StorageSystemNamedCollections::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"collection", std::make_shared<DataTypeString>()},
    };
}

StorageSystemNamedCollections::StorageSystemNamedCollections(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_)
{
}

void StorageSystemNamedCollections::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    context->checkAccess(AccessType::SHOW_NAMED_COLLECTIONS);

    auto collections = NamedCollectionFactory::instance().getAll();

    for (const auto & [name, collection] : collections)
    {
        res_columns[0]->insert(name);
        res_columns[1]->insert(collection->toString());
    }
}

}
