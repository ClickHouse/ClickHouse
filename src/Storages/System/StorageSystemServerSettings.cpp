#include <Storages/System/StorageSystemServerSettings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Core/ServerSettings.h>

namespace DB
{
NamesAndTypesList StorageSystemServerSettings::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeString>()},
        {"default", std::make_shared<DataTypeString>()},
        {"changed", std::make_shared<DataTypeUInt8>()},
        {"description", std::make_shared<DataTypeString>()},
        {"type", std::make_shared<DataTypeString>()},
        {"is_obsolete", std::make_shared<DataTypeUInt8>()},
    };
}

void StorageSystemServerSettings::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    const auto & config = context->getConfigRef();
    ServerSettings settings;
    settings.loadSettingsFromConfig(config);

    for (const auto & setting : settings.all())
    {
        const auto & setting_name = setting.getName();
        res_columns[0]->insert(setting_name);
        res_columns[1]->insert(setting.getValueString());
        res_columns[2]->insert(setting.getDefaultValueString());
        res_columns[3]->insert(setting.isValueChanged());
        res_columns[4]->insert(setting.getDescription());
        res_columns[5]->insert(setting.getTypeName());
        res_columns[6]->insert(setting.isObsolete());
    }
}

}
