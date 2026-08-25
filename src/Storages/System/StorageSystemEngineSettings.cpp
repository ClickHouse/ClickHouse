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
        {"name",         std::make_shared<DataTypeString>(), "Setting name."},
        {"value",        std::make_shared<DataTypeString>(), "Setting value."},
        {"default",      std::make_shared<DataTypeString>(), "Setting default value."},
        {"changed",      std::make_shared<DataTypeUInt8>(), "1 if the setting was explicitly defined in the config or explicitly changed."},
        {"description",  std::make_shared<DataTypeString>(), "Setting description."},
        {"min",          std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Minimum value of the setting, if any is set via constraints. If the setting has no minimum value, contains NULL."},
        {"max",          std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Maximum value of the setting, if any is set via constraints. If the setting has no maximum value, contains NULL."},
        {"disallowed_values", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "List of disallowed values."},
        {"readonly",     std::make_shared<DataTypeUInt8>(),
            "Shows whether the current user can change the setting: "
            "0 - Current user can change the setting, "
            "1 - Current user can't change the setting."
        },
        {"type",         std::make_shared<DataTypeString>(), "Setting type (implementation specific string value)."},
        {"is_obsolete",  std::make_shared<DataTypeUInt8>(), "Shows whether a setting is obsolete."},
        {"tier", getSettingsTierEnum(), R"(
Support level for this feature. ClickHouse features are organized in tiers, varying depending on the current status of their
development and the expectations one might have when using them:
* PRODUCTION: The feature is stable, safe to use and does not have issues interacting with other PRODUCTION features.
* BETA: The feature is stable and safe. The outcome of using it together with other features is unknown and correctness is not guaranteed. Testing and reports are welcome.
* EXPERIMENTAL: The feature is under development. Only intended for developers and ClickHouse enthusiasts. The feature might or might not work and could be removed at any time.
* OBSOLETE: No longer supported. Either it is already removed or it will be removed in future releases.
)"},
    };
}

void StorageSystemEngineSettings::fillData(MutableColumns & res_columns, ContextPtr /*context*/, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & storages = StorageFactory::instance().getAllStorages();

    for (const auto & [engine_name, creator] : storages)
    {
        if (!creator.features.fill_engine_settings_fn)
            continue;

        /// Fill settings for this engine into temporary columns (without engine_name)
        auto num_columns = res_columns.size();
        MutableColumns setting_columns;
        setting_columns.reserve(num_columns - 1);
        for (size_t i = 1; i < num_columns; ++i)
            setting_columns.push_back(res_columns[i]->cloneEmpty());

        creator.features.fill_engine_settings_fn(setting_columns);

        size_t num_rows = setting_columns[0]->size();
        for (size_t row = 0; row < num_rows; ++row)
        {
            res_columns[0]->insert(engine_name);
            for (size_t col = 1; col < num_columns; ++col)
                res_columns[col]->insertFrom(*setting_columns[col - 1], row);
        }
    }
}

}

namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemEngineSettings) }
