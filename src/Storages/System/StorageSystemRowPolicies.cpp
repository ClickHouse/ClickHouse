#include <Storages/System/StorageSystemRowPolicies.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/RowPolicy.h>
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
NamesAndTypesList StorageSystemRowPolicies::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"name", std::make_shared<DataTypeString>()},
        {"short_name", std::make_shared<DataTypeString>()},
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"id", std::make_shared<DataTypeUUID>()},
        {"storage", std::make_shared<DataTypeString>()},
    };

    for (auto filter_type : collections::range(RowPolicyFilterType::MAX))
    {
        const String & column_name = RowPolicyFilterTypeInfo::get(filter_type).name;
        names_and_types.push_back({column_name, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())});
    }

    NamesAndTypesList extra_names_and_types{
        {"is_restrictive", std::make_shared<DataTypeUInt8>()},
        {"apply_to_all", std::make_shared<DataTypeUInt8>()},
        {"apply_to_list", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"apply_to_except", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())}
    };

    insertAtEnd(names_and_types, extra_names_and_types);

    return names_and_types;
}


void StorageSystemRowPolicies::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    context->checkAccess(AccessType::SHOW_ROW_POLICIES);
    const auto & access_control = context->getAccessControl();
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
}
