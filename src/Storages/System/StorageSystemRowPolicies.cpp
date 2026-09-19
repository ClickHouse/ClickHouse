#include <Storages/System/StorageSystemRowPolicies.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/RowPolicy.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/RestorerFromBackup.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/Context.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <base/range.h>
#include <base/insertAtEnd.h>


namespace DB
{
ColumnsDescription StorageSystemRowPolicies::getColumnsDescription()
{
    ColumnsDescription description
    {
        {"name", std::make_shared<DataTypeString>(), "Name of a row policy."},
        {"short_name", std::make_shared<DataTypeString>(),
            "Short name of a row policy. Names of row policies are compound, for example: myfilter ON mydb.mytable. "
            "Here 'myfilter ON mydb.mytable' is the name of the row policy, 'myfilter' is it's short name."
        },
        {"database", std::make_shared<DataTypeString>(), "Database name."},
        {"table", std::make_shared<DataTypeString>(), "Table name. Empty if policy for database."},
        {"id", std::make_shared<DataTypeUUID>(), "Row policy ID."},
        {"storage", std::make_shared<DataTypeString>(), "Name of the directory where the row policy is stored."},
    };

    for (auto filter_type : collections::range(RowPolicyFilterType::MAX))
    {
        const auto & filter_type_info = RowPolicyFilterTypeInfo::get(filter_type);
        description.add({filter_type_info.name, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), filter_type_info.description});
    }

    description.add({"is_restrictive", std::make_shared<DataTypeUInt8>(),
        "Shows whether the row policy restricts access to rows. Value: "
        "• 0 — The row policy is defined with `AS PERMISSIVE` clause, "
        "• 1 — The row policy is defined with AS RESTRICTIVE clause."
    });
    description.add({"apply_to_all", std::make_shared<DataTypeUInt8>(),
        "Shows that the row policies set for all roles and/or users."
    });
    description.add({"apply_to_list", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
        "List of the roles and/or users to which the row policies is applied."
    });
    description.add({"apply_to_except", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
        "The row policies is applied to all roles and/or users excepting of the listed ones."
    });

    return description;
}


void StorageSystemRowPolicies::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    /// If "select_from_system_db_requires_grant" is enabled the access rights were already checked in InterpreterSelectQuery.
    const auto & access_control = context->getAccessControl();
    if (!access_control.doesSelectFromSystemDatabaseRequireGrant())
        context->checkAccess(AccessType::SHOW_ROW_POLICIES);

    std::vector<UUID> ids = access_control.findAll<RowPolicy>();

    size_t column_index = 0;
    auto & column_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_short_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_database = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_table = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_id = assert_cast<ColumnUUID &>(*res_columns[column_index++]).getData();
    auto & column_storage = assert_cast<ColumnString &>(*res_columns[column_index++]);

    ColumnString * column_filter[static_cast<size_t>(RowPolicyFilterType::MAX)];
    NullMap * column_filter_null_map[static_cast<size_t>(RowPolicyFilterType::MAX)];
    for (auto filter_type : collections::range(RowPolicyFilterType::MAX))
    {
        auto filter_type_i = static_cast<size_t>(filter_type);
        column_filter[filter_type_i] = &assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
        column_filter_null_map[filter_type_i] = &assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    }

    auto & column_is_restrictive = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_apply_to_all = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_apply_to_list = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_apply_to_list_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_apply_to_except = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_apply_to_except_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();

    auto add_row = [&](const String & name,
                       const RowPolicyName & full_name,
                       const UUID & id,
                       const String & storage_name,
                       const std::array<String, static_cast<size_t>(RowPolicyFilterType::MAX)> & filters,
                       bool is_restrictive,
                       const RolesOrUsersSet & apply_to)
    {
        column_name.insertData(name.data(), name.length());
        column_short_name.insertData(full_name.short_name.data(), full_name.short_name.length());
        column_database.insertData(full_name.database.data(), full_name.database.length());
        column_table.insertData(full_name.table_name.data(), full_name.table_name.length());
        column_id.push_back(id.toUnderType());
        column_storage.insertData(storage_name.data(), storage_name.length());

        for (auto filter_type : collections::range(RowPolicyFilterType::MAX))
        {
            auto filter_type_i = static_cast<size_t>(filter_type);
            const String & filter = filters[filter_type_i];
            if (filter.empty())
            {
                column_filter[filter_type_i]->insertDefault();
                column_filter_null_map[filter_type_i]->push_back(true);
            }
            else
            {
                column_filter[filter_type_i]->insertData(filter.data(), filter.length());
                column_filter_null_map[filter_type_i]->push_back(false);
            }
        }

        column_is_restrictive.push_back(is_restrictive);

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
        auto policy = access_control.tryRead<RowPolicy>(id);
        if (!policy)
            continue;
        auto storage = access_control.findStorage(id);
        if (!storage)
            continue;

        add_row(policy->getName(), policy->getFullName(), id, storage->getStorageName(), policy->filters, policy->isRestrictive(), policy->to_roles);
    }
}

void StorageSystemRowPolicies::backupData(
    BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    const auto & access_control = backup_entries_collector.getContext()->getAccessControl();
    access_control.backup(backup_entries_collector, data_path_in_backup, AccessEntityType::ROW_POLICY);
}

void StorageSystemRowPolicies::restoreDataFromBackup(
    RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    auto & access_control = restorer.getContext()->getAccessControl();
    access_control.restoreFromBackup(restorer, data_path_in_backup);
}

}
