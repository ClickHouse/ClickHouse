#include <Storages/System/StorageSystemSettingsProfiles.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/SettingsProfile.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/RestorerFromBackup.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/Context.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>


namespace DB
{
ColumnsDescription StorageSystemSettingsProfiles::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Setting profile name."},
        {"id", std::make_shared<DataTypeUUID>(), "Setting profile ID."},
        {"storage", std::make_shared<DataTypeString>(), "Path to the storage of setting profiles. Configured in the `access_control_path` parameter."},
        {"num_elements", std::make_shared<DataTypeUInt64>(), "Number of elements for this profile in the `system.settings_profile_elements` table."},
        {"apply_to_all", std::make_shared<DataTypeUInt8>(), "Shows that the settings profile set for all roles and/or users."},
        {"apply_to_list", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "List of the roles and/or users to which the setting profile is applied."
        },
        {"apply_to_except", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "The setting profile is applied to all roles and/or users excepting of the listed ones."
        },
    };
}


void StorageSystemSettingsProfiles::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    /// If "select_from_system_db_requires_grant" is enabled the access rights were already checked in InterpreterSelectQuery.
    const auto & access_control = context->getAccessControl();
    if (!access_control.doesSelectFromSystemDatabaseRequireGrant())
        context->checkAccess(AccessType::SHOW_SETTINGS_PROFILES);

    std::vector<UUID> ids = access_control.findAll<SettingsProfile>();

    size_t column_index = 0;
    auto & column_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_id = assert_cast<ColumnUUID &>(*res_columns[column_index++]).getData();
    auto & column_storage = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_num_elements = assert_cast<ColumnUInt64 &>(*res_columns[column_index++]).getData();
    auto & column_apply_to_all = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_apply_to_list = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_apply_to_list_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_apply_to_except = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_apply_to_except_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();

    auto add_row = [&](const String & name,
                       const UUID & id,
                       const String & storage_name,
                       const SettingsProfileElements & elements,
                       const RolesOrUsersSet & apply_to)
    {
        column_name.insertData(name.data(), name.length());
        column_id.push_back(id.toUnderType());
        column_storage.insertData(storage_name.data(), storage_name.length());
        column_num_elements.push_back(elements.size());

        auto apply_to_ast = apply_to.toASTWithNames(access_control);
        column_apply_to_all.push_back(apply_to_ast->all);

        for (const auto & role_name : apply_to_ast->names)
            column_apply_to_list.insertData(role_name.data(), role_name.length());
        column_apply_to_list_offsets.push_back(column_apply_to_list.size());

        for (const auto & role_name : apply_to_ast->except_names)
            column_apply_to_except.insertData(role_name.data(), role_name.length());
        column_apply_to_except_offsets.push_back(column_apply_to_except.size());
    };

    for (const auto & id : ids)
    {
        auto profile = access_control.tryRead<SettingsProfile>(id);
        if (!profile)
            continue;

        auto storage = access_control.findStorage(id);
        if (!storage)
            continue;

        add_row(profile->getName(), id, storage->getStorageName(), profile->elements, profile->to_roles);
    }
}

void StorageSystemSettingsProfiles::backupData(
    BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    const auto & access_control = backup_entries_collector.getContext()->getAccessControl();
    access_control.backup(backup_entries_collector, data_path_in_backup, AccessEntityType::SETTINGS_PROFILE);
}

void StorageSystemSettingsProfiles::restoreDataFromBackup(
    RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    auto & access_control = restorer.getContext()->getAccessControl();
    access_control.restoreFromBackup(restorer, data_path_in_backup);
}

}
