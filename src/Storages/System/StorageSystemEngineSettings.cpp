#include <Core/SettingsTierType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/StorageFactory.h>
#include <Storages/System/StorageSystemEngineSettings.h>
#include <Interpreters/Context.h>
#include <Storages/VirtualColumnUtils.h>
#include <Columns/ColumnString.h>
#include <Storages/System/SystemTableSourceRegistry.h>


namespace DB
{

ColumnsDescription StorageSystemEngineSettings::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"engine_name",  std::make_shared<DataTypeString>(), "Name of the table engine."},
        {"name",         std::make_shared<DataTypeString>(), "Setting name."},
        {"value",        std::make_shared<DataTypeString>(), "Value the engine uses on this server. For `MergeTree` and `Distributed` this reflects the server configuration; for engines that have no server-level settings it is the same as `default`."},
        {"default",      std::make_shared<DataTypeString>(), "Value the setting has when nothing configures it."},
        {"changed",      std::make_shared<DataTypeUInt8>(), "1 if something other than the compiled default set this value - the server configuration or the `compatibility` setting. "
            "Not the same as `value` differing from `default`: a configuration section that sets a setting to the value it already had still counts. "
            "Always 0 for engines that have no server-level settings."},
        {"description",  std::make_shared<DataTypeString>(), "Setting description."},
        {"min",          std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Minimum value of the setting, if one is set via the current user's constraints, otherwise NULL. Constraints can only be declared for `MergeTree` settings, so this is NULL for every other engine."},
        {"max",          std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Maximum value of the setting, if one is set via the current user's constraints, otherwise NULL. Constraints can only be declared for `MergeTree` settings, so this is NULL for every other engine."},
        {"disallowed_values", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Values the current user's constraints forbid. Empty for every engine other than `MergeTree`, which is the only one for which constraints can be declared."},
        {"readonly",     std::make_shared<DataTypeUInt8>(),
            "Whether the current user's constraints forbid changing the setting: "
            "0 - no constraint forbids it, "
            "1 - a constraint makes it read-only. "
            "Only `MergeTree` settings can be constrained, so this is 0 for every other engine. "
            "It says nothing about whether the engine accepts `ALTER TABLE ... MODIFY SETTING`."
        },
        {"type",         std::make_shared<DataTypeString>(), "Setting type (implementation specific string value)."},
        {"is_obsolete",  std::make_shared<DataTypeUInt8>(), "Shows whether a setting is obsolete."},
        {"tier", getSettingsTierEnum(), R"(
Support level for this feature. ClickHouse features are organized in tiers, varying depending on the current status of their
development and the expectations one might have when using them:
* PRODUCTION: The feature is stable, safe to use and does not have issues interacting with other PRODUCTION features.
* BETA: The feature is stable and safe. The outcome of using it together with other features is unknown and correctness is not guaranteed. Testing and reports are welcome.
* EXPERIMENTAL: The feature is under development. Only intended for developers and ClickHouse enthusiasts. The feature might or might not work and could be removed at any time.
* PRIVATE PREVIEW: The feature is on a clear path to general availability. Its applicability is still limited and it is not recommended for production use.
* OBSOLETE: No longer supported. Either it is already removed or it will be removed in future releases.
)"},
        {"alias_for",    std::make_shared<DataTypeString>(),
            "Empty on a setting's own row. A setting writable under more than one name also gets a row per other name, "
            "carrying the same values, with this naming the one it is declared under. As in `system.settings`."}
    };
}

Block StorageSystemEngineSettings::getFilterSampleBlock() const
{
    /// Must list every column of the block passed to filterBlockWithPredicate in getFilteredEngines.
    return { { {}, std::make_shared<DataTypeString>(), "engine_name" } };
}

/// The engines a query can still be interested in. Enumerating a settings struct is not free - the
/// `MergeTree` family alone is 366 rows per engine - so a query naming one engine should not pay
/// for the other 56.
static ColumnPtr getFilteredEngines(const StorageFactory::Storages & storages, const ActionsDAG::Node * predicate, ContextPtr context)
{
    MutableColumnPtr engine_column = ColumnString::create();
    for (const auto & [engine_name, creator] : storages)
    {
        if (!creator.features.enumerate_engine_settings_fn || !creator.features.supports_settings)
            continue;
        engine_column->insert(engine_name);
    }

    Block block { ColumnWithTypeAndName(std::move(engine_column), std::make_shared<DataTypeString>(), "engine_name") };
    VirtualColumnUtils::filterBlockWithPredicate(predicate, block, context);
    return block.getByPosition(0).column;
}

void StorageSystemEngineSettings::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8> columns_mask) const
{
    const auto & storages = StorageFactory::instance().getAllStorages();
    const auto filtered_engines = getFilteredEngines(storages, predicate, context);

    for (size_t engine_index = 0; engine_index < filtered_engines->size(); ++engine_index)
    {
        const String engine_name{filtered_engines->getDataAt(engine_index)};
        const auto enumerate = storages.at(engine_name).features.enumerate_engine_settings_fn;

        for (const auto & setting : enumerate(context))
        {
            /// A setting that answers to more than one name gets a row per name, as
            /// `system.settings` does, so that looking it up by the name you happen to know finds
            /// it. The rows carry the same values; `alias_for` tells them apart.
            auto add_row = [&](std::string_view name, std::string_view alias_for)
            {
            size_t src_index = 0;
            size_t res_index = 0;

            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(engine_name);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(name);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(setting.value);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(setting.default_value);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(setting.origin != TableSettingOrigin::Default);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(setting.description);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(setting.min_value ? Field(*setting.min_value) : Field());
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(setting.max_value ? Field(*setting.max_value) : Field());
            if (columns_mask[src_index++])
            {
                Array disallowed;
                disallowed.reserve(setting.disallowed_values.size());
                for (const auto & disallowed_value : setting.disallowed_values)
                    disallowed.emplace_back(disallowed_value);
                res_columns[res_index++]->insert(disallowed);
            }
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(setting.readonly);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(setting.type);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(setting.tier == SettingsTierType::OBSOLETE);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(setting.tier);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(alias_for);
            };

            add_row(setting.name, "");
            for (const auto alias : setting.aliases)
                add_row(alias, setting.name);
        }
    }
}

}

namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemEngineSettings) }
