#include <Storages/System/StorageSystemQuotas.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Access/AccessControlManager.h>
#include <Access/Quota.h>
#include <Access/AccessFlags.h>
#include <ext/range.h>


namespace DB
{
namespace
{
    using KeyType = Quota::KeyType;
    using KeyTypeInfo = Quota::KeyTypeInfo;

    DataTypeEnum8::Values getKeyTypeEnumValues()
    {
        DataTypeEnum8::Values enum_values;
        for (auto key_type : ext::range(KeyType::MAX))
        {
            const auto & type_info = KeyTypeInfo::get(key_type);
            if ((key_type != KeyType::NONE) && type_info.base_types.empty())
                enum_values.push_back({type_info.name, static_cast<Int8>(key_type)});
        }
        return enum_values;
    }
}


NamesAndTypesList StorageSystemQuotas::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"name", std::make_shared<DataTypeString>()},
        {"id", std::make_shared<DataTypeUUID>()},
        {"storage", std::make_shared<DataTypeString>()},
        {"keys", std::make_shared<DataTypeArray>(std::make_shared<DataTypeEnum8>(getKeyTypeEnumValues()))},
        {"durations", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>())},
        {"apply_to_all", std::make_shared<DataTypeUInt8>()},
        {"apply_to_list", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"apply_to_except", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())}
    };
    return names_and_types;
}


void StorageSystemQuotas::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    context->checkAccess(AccessType::SHOW_QUOTAS);
    const auto & access_control = context->getAccessControlManager();
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
                       KeyType key_type,
                       const RolesOrUsersSet & apply_to)
    {
        column_name.insertData(name.data(), name.length());
        column_id.push_back(id.toUnderType());
        column_storage.insertData(storage_name.data(), storage_name.length());

        if (key_type != KeyType::NONE)
        {
            const auto & type_info = KeyTypeInfo::get(key_type);
            for (auto base_type : type_info.base_types)
                column_key_types.push_back(static_cast<Int8>(base_type));
            if (type_info.base_types.empty())
                column_key_types.push_back(static_cast<Int8>(key_type));
        }
        column_key_types_offsets.push_back(column_key_types.size());

        for (const auto & limits : all_limits)
            column_durations.push_back(std::chrono::duration_cast<std::chrono::seconds>(limits.duration).count());
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
}
