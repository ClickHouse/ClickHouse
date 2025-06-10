#include "StorageSystemNamedCollections.h"

#include <Common/FieldVisitorToString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeMap.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Access/Common/AccessType.h>
#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnMap.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>


namespace DB
{

ColumnsDescription StorageSystemNamedCollections::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of the collection."},
        {"collection", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()), "Collection internals."},
    };
}

StorageSystemNamedCollections::StorageSystemNamedCollections(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_, getColumnsDescription())
{
}

void StorageSystemNamedCollections::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & access = context->getAccess();

    NamedCollectionFactory::instance().loadIfNot();

    auto collections = NamedCollectionFactory::instance().getAll();
    for (const auto & [name, collection] : collections)
    {
        if (!access->isGranted(AccessType::SHOW_NAMED_COLLECTIONS, name))
            continue;

        res_columns[0]->insert(name);

        auto * column_map = typeid_cast<ColumnMap *>(res_columns[1].get());

        auto & offsets = column_map->getNestedColumn().getOffsets();
        auto & tuple_column = column_map->getNestedData();
        auto & key_column = tuple_column.getColumn(0);
        auto & value_column = tuple_column.getColumn(1);

        size_t size = 0;
        for (const auto & key : collection->getKeys())
        {
            key_column.insertData(key.data(), key.size());
            if (access->isGranted(AccessType::SHOW_NAMED_COLLECTIONS_SECRETS))
                value_column.insert(collection->get<String>(key));
            else
                value_column.insert("[HIDDEN]");
            size++;
        }

        offsets.push_back(offsets.back() + size);
    }
}

}
