#include <Storages/System/StorageSystemReplicasCollection.h>

#include <base/EnumReflection.h>
#include <Columns/ColumnString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Access/Common/AccessType.h>
#include <Access/ContextAccess.h>
#include <Common/NamedCollections/NamedCollectionReservedKeys.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool format_display_secrets_in_show_and_select;
}

ColumnsDescription StorageSystemReplicasCollection::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Replica collection name."},
        {"host", std::make_shared<DataTypeString>(), "Replica host."},
        {"port", std::make_shared<DataTypeString>(), "Replica TCP port."},
        {"user", std::make_shared<DataTypeString>(), "Replica user."},
        {"password", std::make_shared<DataTypeString>(), "Replica password (masked without secret access)."},
        {"secure", std::make_shared<DataTypeString>(), "Replica secure transport flag."},
        {"compression", std::make_shared<DataTypeString>(), "Replica compression flag."},
        {"priority", std::make_shared<DataTypeString>(), "Replica priority."},
        {"bind_host", std::make_shared<DataTypeString>(), "Replica bind host."},
        {"default_database", std::make_shared<DataTypeString>(), "Replica default database."},
        {"source", std::make_shared<DataTypeString>(), "Named collection source."},
        {"create_query", std::make_shared<DataTypeString>(), "Replica create query."},
    };
}

StorageSystemReplicasCollection::StorageSystemReplicasCollection(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_, getColumnsDescription())
{
}

void StorageSystemReplicasCollection::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & access = context->getAccess();

    NamedCollectionFactory::instance().loadIfNot();
    const auto collections = NamedCollectionFactory::instance().getAll();

    bool access_secrets = access->isGranted(AccessType::SHOW_NAMED_COLLECTIONS_SECRETS);
    access_secrets &= access->isGranted(AccessType::displaySecretsInShowAndSelect);
    access_secrets &= context->getSettingsRef()[Setting::format_display_secrets_in_show_and_select];
    access_secrets &= context->displaySecretsInShowAndSelect();

    for (const auto & [name, collection] : collections)
    {
        /// Discriminator: a named collection is a SQL replica iff it carries the internal
        /// `__type__='replica'` tag injected by `InterpreterCreateReplicaQuery`. The tag is in the
        /// reserved `__` namespace, so no user DDL could have forged it.
        if (collection->getOrDefault<String>(String{NAMED_COLLECTION_KIND_KEY}, "") != NAMED_COLLECTION_KIND_REPLICA)
            continue;
        if (!access->isGranted(AccessType::SHOW_NAMED_COLLECTIONS, name))
            continue;

        res_columns[0]->insert(name);
        res_columns[1]->insert(collection->getOrDefault<String>("host", ""));
        res_columns[2]->insert(collection->getOrDefault<String>("port", ""));
        res_columns[3]->insert(collection->getOrDefault<String>("user", ""));
        res_columns[4]->insert(access_secrets ? collection->getOrDefault<String>("password", "") : String{"[HIDDEN]"});
        res_columns[5]->insert(collection->getOrDefault<String>("secure", ""));
        res_columns[6]->insert(collection->getOrDefault<String>("compression", ""));
        res_columns[7]->insert(collection->getOrDefault<String>("priority", ""));
        res_columns[8]->insert(collection->getOrDefault<String>("bind_host", ""));
        res_columns[9]->insert(collection->getOrDefault<String>("default_database", ""));
        res_columns[10]->insert(magic_enum::enum_name(collection->getSourceId()));
        /// User-facing view: strip the reserved tag from the rendered CREATE statement, same as
        /// `system.named_collections` does, so that copy-pasting `create_query` yields a plain
        /// `CREATE NAMED COLLECTION` that re-rendering (via `CREATE REPLICA`) would produce.
        res_columns[11]->insert(collection->getCreateStatement(/*show_secrets=*/access_secrets, /*hide_reserved_keys=*/true));
    }
}

}
