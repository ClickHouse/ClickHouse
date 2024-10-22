#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Parsers/ASTSetQuery.h>
#include <Interpreters/Context.h>
#include <Access/ContextAccess.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/System/StorageSystemObjectStorageQueueSettings.h>
#include <Storages/ObjectStorageQueue/StorageObjectStorageQueue.h>


namespace DB
{

template <ObjectStorageType type>
ColumnsDescription StorageSystemObjectStorageQueueSettings<type>::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Database of the table with S3Queue Engine."},
        {"table", std::make_shared<DataTypeString>(), "Name of the table with S3Queue Engine."},
        {"name",        std::make_shared<DataTypeString>(), "Setting name."},
        {"value",       std::make_shared<DataTypeString>(), "Setting value."},
        {"type",        std::make_shared<DataTypeString>(), "Setting type (implementation specific string value)."},
        {"changed",     std::make_shared<DataTypeUInt8>(), "1 if the setting was explicitly defined in the config or explicitly changed."},
        {"description", std::make_shared<DataTypeString>(), "Setting description."},
        {"alterable",    std::make_shared<DataTypeUInt8>(),
            "Shows whether the current user can change the setting via ALTER TABLE MODIFY SETTING: "
            "0 — Current user can change the setting, "
            "1 — Current user can't change the setting."
        },
    };
}

template <ObjectStorageType type>
void StorageSystemObjectStorageQueueSettings<type>::fillData(
    MutableColumns & res_columns,
    ContextPtr context,
    const ActionsDAG::Node *,
    std::vector<UInt8>) const
{
    auto add_table = [&](
        const DatabaseTablesIteratorPtr & it, StorageObjectStorageQueue & storage)
    {
        if (storage.getType() != type)
            return;

        /// We cannot use setting.isValueChanged(), because we do not store initial settings in storage.
        /// Therefore check if the setting was changed via table metadata.
        const auto & settings_changes = storage.getInMemoryMetadataPtr()->settings_changes->as<ASTSetQuery>()->changes;
        auto is_changed = [&](const std::string & setting_name) -> bool
        {
            return settings_changes.end() != std::find_if(
                settings_changes.begin(), settings_changes.end(),
                [&](const SettingChange & change){ return change.name == setting_name; });
        };

        for (const auto & change : storage.getSettings())
        {
            size_t i = 0;
            res_columns[i++]->insert(it->databaseName());
            res_columns[i++]->insert(it->name());
            res_columns[i++]->insert(change.getName());
            res_columns[i++]->insert(convertFieldToString(change.getValue()));
            res_columns[i++]->insert(change.getTypeName());
            res_columns[i++]->insert(is_changed(change.getName()));
            res_columns[i++]->insert(change.getDescription());
            res_columns[i++]->insert(false);
        }
    };

    const auto access = context->getAccess();
    const bool show_tables_granted = access->isGranted(AccessType::SHOW_TABLES);
    if (show_tables_granted)
    {
        auto databases = DatabaseCatalog::instance().getDatabases();
        for (const auto & db : databases)
        {
            for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
            {
                StoragePtr storage = iterator->table();
                if (auto * queue_table = dynamic_cast<StorageObjectStorageQueue *>(storage.get()))
                {
                    add_table(iterator, *queue_table);
                }
            }
        }

    }
}

template class StorageSystemObjectStorageQueueSettings<ObjectStorageType::S3>;
template class StorageSystemObjectStorageQueueSettings<ObjectStorageType::Azure>;
}
