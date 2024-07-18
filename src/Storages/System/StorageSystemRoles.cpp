#include <Storages/System/StorageSystemRoles.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Access/AccessControl.h>
#include <Access/Role.h>
#include <Access/Common/AccessFlags.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/RestorerFromBackup.h>
#include <Interpreters/Context.h>


namespace DB
{

NamesAndTypesList StorageSystemRoles::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"name", std::make_shared<DataTypeString>()},
        {"id", std::make_shared<DataTypeUUID>()},
        {"storage", std::make_shared<DataTypeString>()},
    };
    return names_and_types;
}


void StorageSystemRoles::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    /// If "select_from_system_db_requires_grant" is enabled the access rights were already checked in InterpreterSelectQuery.
    const auto & access_control = context->getAccessControl();
    if (!access_control.doesSelectFromSystemDatabaseRequireGrant())
        context->checkAccess(AccessType::SHOW_ROLES);

    std::vector<UUID> ids = access_control.findAll<Role>();

    size_t column_index = 0;
    auto & column_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_id = assert_cast<ColumnUUID &>(*res_columns[column_index++]).getData();
    auto & column_storage = assert_cast<ColumnString &>(*res_columns[column_index++]);

    auto add_row = [&](const String & name,
                       const UUID & id,
                       const String & storage_name)
    {
        column_name.insertData(name.data(), name.length());
        column_id.push_back(id.toUnderType());
        column_storage.insertData(storage_name.data(), storage_name.length());
    };

    for (const auto & id : ids)
    {
        auto role = access_control.tryRead<Role>(id);
        if (!role)
            continue;

        auto storage = access_control.findStorage(id);
        if (!storage)
            continue;

        add_row(role->getName(), id, storage->getStorageName());
    }
}

void StorageSystemRoles::backupData(
    BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    const auto & access_control = backup_entries_collector.getContext()->getAccessControl();
    access_control.backup(backup_entries_collector, data_path_in_backup, AccessEntityType::ROLE);
}

void StorageSystemRoles::restoreDataFromBackup(
    RestorerFromBackup & restorer, const String & /* data_path_in_backup */, const std::optional<ASTs> & /* partitions */)
{
    auto & access_control = restorer.getContext()->getAccessControl();
    access_control.restoreFromBackup(restorer);
}

}
