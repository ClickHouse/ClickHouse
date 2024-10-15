#include <Storages/System/StorageSystemQuotas.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/Quota.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/RestorerFromBackup.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/Context.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <base/range.h>


namespace DB
{
namespace
{
    DataTypeEnum8::Values getKeyTypeEnumValues()
    {
        DataTypeEnum8::Values enum_values;
        for (auto key_type : collections::range(QuotaKeyType::MAX))
        {
            const auto & type_info = QuotaKeyTypeInfo::get(key_type);
            if ((key_type != QuotaKeyType::NONE) && type_info.base_types.empty())
                enum_values.push_back({type_info.name, static_cast<Int8>(key_type)});
        }
        return enum_values;
    }
}


ColumnsDescription StorageSystemQuotas::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Quota name."},
        {"id", std::make_shared<DataTypeUUID>(), "Quota ID."},
        {"storage", std::make_shared<DataTypeString>(), "Storage of quotas. Possible value: “users.xml” if a quota configured in the users.xml file, “disk” if a quota configured by an SQL-query."},
        {"keys", std::make_shared<DataTypeArray>(std::make_shared<DataTypeEnum8>(getKeyTypeEnumValues())),
            "Key specifies how the quota should be shared. If two connections use the same quota and key, they share the same amounts of resources. Values: "
            "[] — All users share the same quota, "
            "['user_name'] — Connections with the same user name share the same quota, "
            "['ip_address'] — Connections from the same IP share the same quota. "
            "['client_key'] — Connections with the same key share the same quota. A key must be explicitly provided by a client. "
            "When using clickhouse-client, pass a key value in the --quota_key parameter, "
            "or use the quota_key parameter in the client configuration file. "
            "When using HTTP interface, use the X-ClickHouse-Quota header, "
            "['user_name', 'client_key'] — Connections with the same client_key share the same quota. If a key isn't provided by a client, the quota is tracked for `user_name`, "
            "['client_key', 'ip_address'] — Connections with the same client_key share the same quota. If a key isn’t provided by a client, the quota is tracked for ip_address."
        },
        {"durations", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>()), "Time interval lengths in seconds."},
        {"apply_to_all", std::make_shared<DataTypeUInt8>(),
            "Logical value. It shows which users the quota is applied to. Values: "
            "0 — The quota applies to users specify in the apply_to_list. "
            "1 — The quota applies to all users except those listed in apply_to_except."
        },
        {"apply_to_list", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "List of user names/roles that the quota should be applied to."},
        {"apply_to_except", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "List of user names/roles that the quota should not apply to."}
    };
}


void StorageSystemQuotas::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    /// If "select_from_system_db_requires_grant" is enabled the access rights were already checked in InterpreterSelectQuery.
    const auto & access_control = context->getAccessControl();
    if (!access_control.doesSelectFromSystemDatabaseRequireGrant())
        context->checkAccess(AccessType::SHOW_QUOTAS);

    std::vector<UUID> ids = access_control.findAll<Quota>();

    size_t column_index = 0;
    auto & column_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_id = assert_cast<ColumnUUID &>(*res_columns[column_index++]).getData();
    auto & column_storage = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_key_types = assert_cast<ColumnInt8 &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData()).getData();
    auto & column_key_types_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_durations = assert_cast<ColumnUInt32 &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData()).getData();
    auto & column_durations_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_apply_to_all = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_apply_to_list = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_apply_to_list_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_apply_to_except = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_apply_to_except_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();

    auto add_row = [&](const String & name,
                       const UUID & id,
                       const String & storage_name,
                       const std::vector<Quota::Limits> & all_limits,
                       QuotaKeyType key_type,
                       const RolesOrUsersSet & apply_to)
    {
        column_name.insertData(name.data(), name.length());
        column_id.push_back(id.toUnderType());
        column_storage.insertData(storage_name.data(), storage_name.length());

        if (key_type != QuotaKeyType::NONE)
        {
            const auto & type_info = QuotaKeyTypeInfo::get(key_type);
            for (auto base_type : type_info.base_types)
                column_key_types.push_back(static_cast<Int8>(base_type));
            if (type_info.base_types.empty())
                column_key_types.push_back(static_cast<Int8>(key_type));
        }
        column_key_types_offsets.push_back(column_key_types.size());

        for (const auto & limits : all_limits)
        {
            column_durations.push_back(
                static_cast<UInt32>(std::chrono::duration_cast<std::chrono::seconds>(limits.duration).count()));
        }
        column_durations_offsets.push_back(column_durations.size());

        auto apply_to_ast = apply_to.toASTWithNames(access_control);
        column_apply_to_all.push_back(apply_to_ast->all);

        for (const auto & role_name : apply_to_ast->names)
            column_apply_to_list.insertData(role_name.data(), role_name.length());
        column_apply_to_list_offsets.push_back(column_apply_to_list.size());

        for (const auto & role_name : apply_to_ast->except_names)
            column_apply_to_except.insertData(role_name.data(), role_name.length());
        column_apply_to_except_offsets.push_back(column_apply_to_except.size());
    };

    for (const auto & id : access_control.findAll<Quota>())
    {
        auto quota = access_control.tryRead<Quota>(id);
        if (!quota)
            continue;
        auto storage = access_control.findStorage(id);
        if (!storage)
            continue;

        add_row(quota->getName(), id, storage->getStorageName(), quota->all_limits, quota->key_type, quota->to_roles);
    }
}

void StorageSystemQuotas::backupData(
    BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    const auto & access_control = backup_entries_collector.getContext()->getAccessControl();
    access_control.backup(backup_entries_collector, data_path_in_backup, AccessEntityType::QUOTA);
}

void StorageSystemQuotas::restoreDataFromBackup(
    RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    auto & access_control = restorer.getContext()->getAccessControl();
    access_control.restoreFromBackup(restorer, data_path_in_backup);
}

}
