#include <Storages/System/StorageSystemSettingsProfileElements.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <Access/AccessControl.h>
#include <Access/Role.h>
#include <Access/User.h>
#include <Access/SettingsProfile.h>
#include <Interpreters/Context.h>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <Common/SettingConstraintWritability.h>
#include <base/range.h>


namespace DB
{

const std::vector<std::pair<String, Int8>> & getSettingConstraintWritabilityEnumValues()
{
    static const std::vector<std::pair<String, Int8>> values = []
    {
        std::vector<std::pair<String, Int8>> res;
        for (auto value : collections::range(SettingConstraintWritability::MAX))
            res.emplace_back(toString(value), static_cast<Int8>(value));
        return res;
    }();
    return values;
}

ColumnsDescription StorageSystemSettingsProfileElements::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"profile_name", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Setting profile name."},
        {"user_name", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "User name."},
        {"role_name", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Role name."},
        {"index", std::make_shared<DataTypeUInt64>(), "Sequential number of the settings profile element."},
        {"setting_name", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Setting name."},
        {"value", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Setting value."},
        {"min", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "The minimum value of the setting. NULL if not set."},
        {"max", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "The maximum value of the setting. NULL if not set."},
        {"writability", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeEnum8>(getSettingConstraintWritabilityEnumValues())), "The property which shows whether a setting can be changed or not."},
        {"inherit_profile", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
            "A parent profile for this setting profile. NULL if not set. "
            "Setting profile will inherit all the settings' values and constraints (min, max, readonly) from its parent profiles."
        },
    };
}


void StorageSystemSettingsProfileElements::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    /// If "select_from_system_db_requires_grant" is enabled the access rights were already checked in InterpreterSelectQuery.
    const auto & access_control = context->getAccessControl();
    if (!access_control.doesSelectFromSystemDatabaseRequireGrant())
        context->checkAccess(AccessType::SHOW_SETTINGS_PROFILES);

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
    auto & column_writability = assert_cast<ColumnInt8 &>(assert_cast<ColumnNullable &>(*res_columns[i]).getNestedColumn());
    auto & column_writability_null_map = assert_cast<ColumnNullable &>(*res_columns[i++]).getNullMapData();
    auto & column_inherit_profile = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[i]).getNestedColumn());
    auto & column_inherit_profile_null_map = assert_cast<ColumnNullable &>(*res_columns[i++]).getNullMapData();

    auto add_rows_for_single_element = [&](const String & owner_name, AccessEntityType owner_type, const SettingsProfileElement & element, size_t & index)
    {
        size_t old_num_rows = column_profile_name.size();
        size_t new_num_rows = old_num_rows + 1;
        size_t current_index = index++;

        bool inserted_value = false;
        if (element.value && !element.setting_name.empty())
        {
            String str = Settings::valueToStringUtil(element.setting_name, *element.value);
            column_value.insertData(str.data(), str.length());
            column_value_null_map.push_back(false);
            inserted_value = true;
        }

        bool inserted_min = false;
        if (element.min_value && !element.setting_name.empty())
        {
            String str = Settings::valueToStringUtil(element.setting_name, *element.min_value);
            column_min.insertData(str.data(), str.length());
            column_min_null_map.push_back(false);
            inserted_min = true;
        }

        bool inserted_max = false;
        if (element.max_value && !element.setting_name.empty())
        {
            String str = Settings::valueToStringUtil(element.setting_name, *element.max_value);
            column_max.insertData(str.data(), str.length());
            column_max_null_map.push_back(false);
            inserted_max = true;
        }

        bool inserted_writability = false;
        if (element.writability && !element.setting_name.empty())
        {
            column_writability.insertValue(static_cast<Int8>(*element.writability));
            column_writability_null_map.push_back(false);
            inserted_writability = true;
        }

        bool inserted_setting_name = false;
        if (inserted_value || inserted_min || inserted_max || inserted_writability)
        {
            const auto & setting_name = element.setting_name;
            column_setting_name.insertData(setting_name.data(), setting_name.size());
            column_setting_name_null_map.push_back(false);
            inserted_setting_name = true;
        }

        bool inserted_inherit_profile = false;
        if (element.parent_profile)
        {
            auto parent_profile = access_control.tryReadName(*element.parent_profile);
            if (parent_profile)
            {
                const String & parent_profile_str = *parent_profile;
                column_inherit_profile.insertData(parent_profile_str.data(), parent_profile_str.length());
                column_inherit_profile_null_map.push_back(false);
                inserted_inherit_profile = true;
            }
        }

        if (inserted_setting_name || inserted_inherit_profile)
        {
            switch (owner_type)
            {
                case AccessEntityType::SETTINGS_PROFILE:
                {
                    column_profile_name.insertData(owner_name.data(), owner_name.length());
                    column_profile_name_null_map.push_back(false);
                    break;
                }
                case AccessEntityType::USER:
                {
                    column_user_name.insertData(owner_name.data(), owner_name.length());
                    column_user_name_null_map.push_back(false);
                    break;
                }
                case AccessEntityType::ROLE:
                {
                    column_role_name.insertData(owner_name.data(), owner_name.length());
                    column_role_name_null_map.push_back(false);
                    break;
                }
                default:
                    assert(false);
            }

            column_index.push_back(current_index);

            for (auto & res_column : res_columns)
                res_column->insertManyDefaults(new_num_rows - res_column->size());
        }
    };

    auto add_rows = [&](const String & owner_name, AccessEntityType owner_type, const SettingsProfileElements & elements)
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
