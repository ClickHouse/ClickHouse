#include <Storages/System/StorageSystemPrivileges.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/SettingsProfile.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>


namespace DB
{
namespace
{
    enum Level
    {
        GROUP = -1,
        GLOBAL,
        DATABASE,
        TABLE,
        DICTIONARY,
        VIEW,
        COLUMN,
        NAMED_COLLECTION,
        USER_NAME,
        TABLE_ENGINE,
    };

    DataTypeEnum8::Values getLevelEnumValues()
    {
        DataTypeEnum8::Values enum_values;
        enum_values.emplace_back("GLOBAL", static_cast<Int8>(GLOBAL));
        enum_values.emplace_back("DATABASE", static_cast<Int8>(DATABASE));
        enum_values.emplace_back("TABLE", static_cast<Int8>(TABLE));
        enum_values.emplace_back("DICTIONARY", static_cast<Int8>(DICTIONARY));
        enum_values.emplace_back("VIEW", static_cast<Int8>(VIEW));
        enum_values.emplace_back("COLUMN", static_cast<Int8>(COLUMN));
        enum_values.emplace_back("NAMED_COLLECTION", static_cast<Int8>(NAMED_COLLECTION));
        enum_values.emplace_back("USER_NAME", static_cast<Int8>(USER_NAME));
        enum_values.emplace_back("TABLE_ENGINE", static_cast<Int8>(TABLE_ENGINE));
        return enum_values;
    }
}


const std::vector<std::pair<String, Int16>> & StorageSystemPrivileges::getAccessTypeEnumValues()
{
    static const std::vector<std::pair<String, Int16>> values = []
    {
        std::vector<std::pair<String, Int16>> res;

#define ADD_ACCESS_TYPE_ENUM_VALUE(name, aliases, node_type, parent_group_name) \
        res.emplace_back(toString(AccessType::name), static_cast<size_t>(AccessType::name));

        APPLY_FOR_ACCESS_TYPES(ADD_ACCESS_TYPE_ENUM_VALUE)
#undef ADD_ACCESS_TYPE_ENUM_VALUE

        return res;
    }();
    return values;
}


ColumnsDescription StorageSystemPrivileges::getColumnsDescription()
{
    return ColumnsDescription{
        {"privilege",
         std::make_shared<DataTypeEnum16>(getAccessTypeEnumValues()),
         "Name of a privilege which can be used in the GRANT command."},
        {"aliases",
         std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
         "List of aliases which can be used instead of the name of the privilege."},
        {"level",
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeEnum8>(getLevelEnumValues())),
         "Level of the privilege. GLOBAL privileges can be granted only globally (ON *.*), DATABASE privileges can be granted "
         "on a specific database (ON <database>.*) or globally (ON *.*), TABLE privileges can be granted either on a specific table or "
         "on a specific database or globally, and COLUMN privileges can be granted like TABLE privileges but also allow to specify columns."},
        {"parent_group", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeEnum16>(getAccessTypeEnumValues())),
         "Parent privilege - if the parent privilege is granted then all its children privileges are considered as granted too."
        },
    };
}


void StorageSystemPrivileges::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    size_t column_index = 0;
    auto & column_access_type = assert_cast<ColumnInt16 &>(*res_columns[column_index++]).getData();
    auto & column_aliases = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_aliases_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_level = assert_cast<ColumnInt8 &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn()).getData();
    auto & column_level_null_map = assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    auto & column_parent_group = assert_cast<ColumnInt16 &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn()).getData();
    auto & column_parent_group_null_map = assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();

    auto add_row = [&](AccessType access_type, std::string_view aliases, Level max_level, AccessType parent_group)
    {
        column_access_type.push_back(static_cast<Int16>(access_type));

        for (size_t pos = 0; pos < aliases.length();)
        {
            size_t next_pos = aliases.find_first_of(',', pos);
            std::string_view alias = aliases.substr(pos, next_pos - pos);
            pos = ((next_pos == std::string_view::npos) ? next_pos : next_pos + 1);

            while (alias.starts_with(' '))
                alias.remove_prefix(1);
            while (alias.ends_with(' '))
                alias.remove_suffix(1);
            column_aliases.insertData(alias.data(), alias.length());
        }
        column_aliases_offsets.push_back(column_aliases.size());

        if (max_level == GROUP)
        {
            column_level.push_back(0);
            column_level_null_map.push_back(true);
        }
        else
        {
            column_level.push_back(static_cast<Int8>(max_level));
            column_level_null_map.push_back(false);
        }

        if (parent_group == AccessType::NONE)
        {
            column_parent_group.push_back(0);
            column_parent_group_null_map.push_back(true);
        }
        else
        {
            column_parent_group.push_back(static_cast<Int16>(parent_group));
            column_parent_group_null_map.push_back(false);
        }
    };

#define STORAGE_SYSTEM_PRIVILEGES_ADD_ROW(name, aliases, node_type, parent_group_name) \
    add_row(AccessType::name, aliases, node_type, AccessType::parent_group_name);

    APPLY_FOR_ACCESS_TYPES(STORAGE_SYSTEM_PRIVILEGES_ADD_ROW)

#undef STORAGE_SYSTEM_PRIVILEGES_ADD_ROW
}

}
