#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemMergeTreeSettings.h>
#include <Access/SettingsConstraintsAndProfileIDs.h>


namespace DB
{

template <bool replicated>
NamesAndTypesList SystemMergeTreeSettings<replicated>::getNamesAndTypes()
{
    return {
        {"name",        std::make_shared<DataTypeString>()},
        {"value",       std::make_shared<DataTypeString>()},
        {"changed",     std::make_shared<DataTypeUInt8>()},
        {"description", std::make_shared<DataTypeString>()},
        {"min",         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"max",         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"readonly",    std::make_shared<DataTypeUInt8>()},
        {"type",        std::make_shared<DataTypeString>()},
        {"is_obsolete", std::make_shared<DataTypeUInt8>()},
    };
}

template <bool replicated>
void SystemMergeTreeSettings<replicated>::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    const auto & settings = replicated ? context->getReplicatedMergeTreeSettings() : context->getMergeTreeSettings();
    auto constraints_and_current_profiles = context->getSettingsConstraintsAndCurrentProfiles();
    const auto & constraints = constraints_and_current_profiles->constraints;
    for (const auto & setting : settings.all())
    {
        const auto & setting_name = setting.getName();
        res_columns[0]->insert(setting_name);
        res_columns[1]->insert(setting.getValueString());
        res_columns[2]->insert(setting.isValueChanged());
        res_columns[3]->insert(setting.getDescription());

        Field min, max;
        SettingConstraintWritability writability = SettingConstraintWritability::WRITABLE;
        constraints.get(settings, setting_name, min, max, writability);

        /// These two columns can accept strings only.
        if (!min.isNull())
            min = Settings::valueToStringUtil(setting_name, min);
        if (!max.isNull())
            max = Settings::valueToStringUtil(setting_name, max);

        res_columns[4]->insert(min);
        res_columns[5]->insert(max);
        res_columns[6]->insert(writability == SettingConstraintWritability::CONST);
        res_columns[7]->insert(setting.getTypeName());
        res_columns[8]->insert(setting.isObsolete());
    }
}

template class SystemMergeTreeSettings<false>;
template class SystemMergeTreeSettings<true>;
}
