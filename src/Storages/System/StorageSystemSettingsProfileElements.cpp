#include <Storages/System/StorageSystemSettingsProfileElements.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Access/AccessControlManager.h>
#include <Access/Role.h>
#include <Access/User.h>
#include <Access/SettingsProfile.h>
#include <Interpreters/Context.h>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{
using EntityType = IAccessEntity::Type;


NamesAndTypesList StorageSystemSettingsProfileElements::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"profile_name", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"user_name", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"role_name", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"index", std::make_shared<DataTypeUInt64>()},
        {"setting_name", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"value", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"min", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"max", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"readonly", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>())},
        {"inherit_profile", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
    };
    return names_and_types;
}


void StorageSystemSettingsProfileElements::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    context.checkAccess(AccessType::SHOW_SETTINGS_PROFILES);
    const auto & access_control = context.getAccessControlManager();
    std::vector<UUID> ids = access_control.findAll<User>();
    boost::range::push_back(ids, access_control.findAll<Role>());
    boost::range::push_back(ids, access_control.findAll<SettingsProfile>());

    size_t i = 0;
    auto & column_profile_name = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[i]).getNestedColumn());
    auto & column_profile_name_null_map = assert_cast<ColumnNullable &>(*res_columns[i++]).getNullMapData();
    auto & column_user_name = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[i]).getNestedColumn());
    auto & column_user_name_null_map = assert_cast<ColumnNullable &>(*res_columns[i++]).getNullMapData();
    auto & column_role_name = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[i]).getNestedColumn());
    auto & column_role_name_null_map = assert_cast<ColumnNullable &>(*res_columns[i++]).getNullMapData();
    auto & column_index = assert_cast<ColumnUInt64 &>(*res_columns[i++]).getData();
    auto & column_setting_name = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[i]).getNestedColumn());
    auto & column_setting_name_null_map = assert_cast<ColumnNullable &>(*res_columns[i++]).getNullMapData();
    auto & column_value = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[i]).getNestedColumn());
    auto & column_value_null_map = assert_cast<ColumnNullable &>(*res_columns[i++]).getNullMapData();
    auto & column_min = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[i]).getNestedColumn());
    auto & column_min_null_map = assert_cast<ColumnNullable &>(*res_columns[i++]).getNullMapData();
    auto & column_max = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[i]).getNestedColumn());
    auto & column_max_null_map = assert_cast<ColumnNullable &>(*res_columns[i++]).getNullMapData();
    auto & column_readonly = assert_cast<ColumnUInt8 &>(assert_cast<ColumnNullable &>(*res_columns[i]).getNestedColumn()).getData();
    auto & column_readonly_null_map = assert_cast<ColumnNullable &>(*res_columns[i++]).getNullMapData();
    auto & column_inherit_profile = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[i]).getNestedColumn());
    auto & column_inherit_profile_null_map = assert_cast<ColumnNullable &>(*res_columns[i++]).getNullMapData();

    auto add_rows_for_single_element = [&](const String & owner_name, EntityType owner_type, const SettingsProfileElement & element, size_t & index)
    {
        switch (owner_type)
        {
            case EntityType::SETTINGS_PROFILE:
            {
                column_user_name.insertDefault();
                column_user_name_null_map.push_back(true);
                column_role_name.insertDefault();
                column_role_name_null_map.push_back(true);
                column_profile_name.insertData(owner_name.data(), owner_name.length());
                column_profile_name_null_map.push_back(false);
                break;
            }
            case EntityType::USER:
            {
                column_user_name.insertData(owner_name.data(), owner_name.length());
                column_user_name_null_map.push_back(false);
                column_profile_name.insertDefault();
                column_profile_name_null_map.push_back(true);
                column_role_name.insertDefault();
                column_role_name_null_map.push_back(true);
                break;
            }
            case EntityType::ROLE:
            {
                column_user_name.insertDefault();
                column_user_name_null_map.push_back(true);
                column_role_name.insertData(owner_name.data(), owner_name.length());
                column_role_name_null_map.push_back(false);
                column_profile_name.insertDefault();
                column_profile_name_null_map.push_back(true);
                break;
            }
            default:
                assert(false);
        }

        if (element.parent_profile)
        {
            auto parent_profile = access_control.tryReadName(*element.parent_profile);
            if (parent_profile)
            {
                column_index.push_back(index++);
                column_setting_name.insertDefault();
                column_setting_name_null_map.push_back(true);
                column_value.insertDefault();
                column_value_null_map.push_back(true);
                column_min.insertDefault();
                column_min_null_map.push_back(true);
                column_max.insertDefault();
                column_max_null_map.push_back(true);
                column_readonly.push_back(0);
                column_readonly_null_map.push_back(true);
                const String & parent_profile_str = *parent_profile;
                column_inherit_profile.insertData(parent_profile_str.data(), parent_profile_str.length());
                column_inherit_profile_null_map.push_back(false);
            }
        }

        if (!element.setting_name.empty()
            && (!element.value.isNull() || !element.min_value.isNull() || !element.max_value.isNull() || element.readonly))
        {
            const auto & setting_name = element.setting_name;
            column_index.push_back(index++);
            column_setting_name.insertData(setting_name.data(), setting_name.size());
            column_setting_name_null_map.push_back(false);

            if (element.value.isNull())
            {
                column_value.insertDefault();
                column_value_null_map.push_back(true);
            }
            else
            {
                String str = Settings::valueToStringUtil(setting_name, element.value);
                column_value.insertData(str.data(), str.length());
                column_value_null_map.push_back(false);
            }

            if (element.min_value.isNull())
            {
                column_min.insertDefault();
                column_min_null_map.push_back(true);
            }
            else
            {
                String str = Settings::valueToStringUtil(setting_name, element.min_value);
                column_min.insertData(str.data(), str.length());
                column_min_null_map.push_back(false);
            }

            if (element.max_value.isNull())
            {
                column_max.insertDefault();
                column_max_null_map.push_back(true);
            }
            else
            {
                String str = Settings::valueToStringUtil(setting_name, element.max_value);
                column_max.insertData(str.data(), str.length());
                column_max_null_map.push_back(false);
            }

            if (element.readonly)
            {
                column_readonly.push_back(*element.readonly);
                column_readonly_null_map.push_back(false);
            }
            else
            {
                column_readonly.push_back(0);
                column_readonly_null_map.push_back(true);
            }

            column_inherit_profile.insertDefault();
            column_inherit_profile_null_map.push_back(true);
        }
    };

    auto add_rows = [&](const String & owner_name, IAccessEntity::Type owner_type, const SettingsProfileElements & elements)
    {
        size_t index = 0;
        for (const auto & element : elements)
            add_rows_for_single_element(owner_name, owner_type, element, index);
    };

    for (const auto & id : ids)
    {
        auto entity = access_control.tryRead(id);
        if (!entity)
            continue;

        const SettingsProfileElements * settings = nullptr;
        if (auto role = typeid_cast<RolePtr>(entity))
            settings = &role->settings;
        else if (auto user = typeid_cast<UserPtr>(entity))
            settings = &user->settings;
        else if (auto profile = typeid_cast<SettingsProfilePtr>(entity))
            settings = &profile->elements;
        else
            continue;

        add_rows(entity->getName(), entity->getType(), *settings);
    }
}

}
