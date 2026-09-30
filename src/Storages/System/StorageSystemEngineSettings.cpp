#include <Core/SettingsTierType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/StorageFactory.h>
#include <Storages/System/StorageSystemEngineSettings.h>
#include <Storages/System/SystemTableSourceRegistry.h>


namespace DB
{

ColumnsDescription StorageSystemEngineSettings::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"engine_name",  std::make_shared<DataTypeString>(), "Name of the table engine."},
        {"name",        std::make_shared<DataTypeString>(), "Setting name."},
        {"value",       std::make_shared<DataTypeString>(), "Setting value."},
        {"default",     std::make_shared<DataTypeString>(), "Setting default value."},
        {"changed",     std::make_shared<DataTypeUInt8>(), "1 if the setting was explicitly defined in the config or explicitly changed."},
        {"description", std::make_shared<DataTypeString>(), "Setting description."},
        {"min",         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Minimum value of the setting, if any is set via constraints. If the setting has no minimum value, contains NULL."},
        {"max",         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Maximum value of the setting, if any is set via constraints. If the setting has no maximum value, contains NULL."},
        {"disallowed_values",         std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "List of disallowed values"},
        {"readonly",    std::make_shared<DataTypeUInt8>(),
            "Shows whether the current user can change the setting: "
            "0 — Current user can change the setting, "
            "1 — Current user can't change the setting."
        },
        {"type",        std::make_shared<DataTypeString>(), "Setting type (implementation specific string value)."},
        {"is_obsolete", std::make_shared<DataTypeUInt8>(), "Shows whether a setting is obsolete."},
        {"tier", getSettingsTierEnum(), R"(
Support level for this feature. ClickHouse features are organized in tiers, varying depending on the current status of their
development and the expectations one might have when using them:
* PRODUCTION: The feature is stable, safe to use and does not have issues interacting with other PRODUCTION features.
* BETA: The feature is stable and safe. The outcome of using it together with other features is unknown and correctness is not guaranteed. Testing and reports are welcome.
* EXPERIMENTAL: The feature is under development. Only intended for developers and ClickHouse enthusiasts. The feature might or might not work and could be removed at any time.
* PRIVATE PREVIEW: The feature is on a clear path to general availability. Its applicability is still limited and it is not recommended for production use.
* OBSOLETE: No longer supported. Either it is already removed or it will be removed in future releases.
)"},
    };
}

void StorageSystemEngineSettings::fillData(MutableColumns & res_columns, ContextPtr /*context*/, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (const auto & [engine_name, creator] : StorageFactory::instance().getAllStorages())
    {
        if (!creator.features.enumerate_engine_settings_fn)
            continue;

        for (const auto & setting : creator.features.enumerate_engine_settings_fn())
        {
            size_t col = 0;
            res_columns[col++]->insert(engine_name);
            res_columns[col++]->insert(setting.name);
            res_columns[col++]->insert(setting.value);
            res_columns[col++]->insert(setting.default_value);
            res_columns[col++]->insert(setting.changed);
            res_columns[col++]->insert(setting.comment);
            res_columns[col++]->insertDefault(); // min (NULL)
            res_columns[col++]->insertDefault(); // max (NULL)
            res_columns[col++]->insert(Array{}); // disallowed_values
            res_columns[col++]->insert(UInt64(0)); // readonly
            res_columns[col++]->insert(setting.type);
            res_columns[col++]->insert(setting.tier == SettingsTierType::OBSOLETE);
            res_columns[col++]->insert(setting.tier);
        }
    }
}

}

namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemEngineSettings) }
