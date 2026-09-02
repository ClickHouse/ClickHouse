#include <Core/SettingsTierType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/StorageFactory.h>
#include <Storages/System/StorageSystemEngineSettings.h>
#include <Storages/System/MutableColumnsAndConstraints.h>
#include <Access/SettingsConstraintsAndProfileIDs.h>
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
        {"changed",      std::make_shared<DataTypeUInt8>(), "1 if `value` differs from `default`, i.e. the server configuration or the `compatibility` setting changed it. Always 0 for engines that have no server-level settings."},
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
* OBSOLETE: No longer supported. Either it is already removed or it will be removed in future releases.
)"},
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
        if (!creator.features.fill_engine_settings_fn || !creator.features.supports_settings)
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

    const auto constraints_and_current_profiles = context->getSettingsConstraintsAndCurrentProfiles();
    const auto & constraints = constraints_and_current_profiles->constraints;

    /// A fill function writes every setting column, so the columns it fills into are built from the
    /// full description rather than from `res_columns`, which holds only the queried ones. The mask
    /// then decides which of them reach the result. It cannot make a fill function do less work:
    /// `MergeTreeSettings` shares `dumpToSystemMergeTreeSettingsColumns` with
    /// `system.merge_tree_settings`, which has no mask to pass on.
    const auto all_columns = getColumnsDescription().getAllPhysical();

    const auto filtered_engines = getFilteredEngines(storages, predicate, context);

    for (size_t engine_index = 0; engine_index < filtered_engines->size(); ++engine_index)
    {
        const String engine_name{filtered_engines->getDataAt(engine_index)};
        const auto fill_fn = storages.at(engine_name).features.fill_engine_settings_fn;

        /// Every column except `engine_name`, which is per engine rather than per setting.
        MutableColumns setting_columns;
        setting_columns.reserve(all_columns.size() - 1);
        for (auto it = std::next(all_columns.begin()); it != all_columns.end(); ++it)
            setting_columns.push_back(it->type->createColumn());

        MutableColumnsAndConstraints params(setting_columns, constraints);
        fill_fn(params, context);

        const size_t num_rows = setting_columns[0]->size();
        size_t src_index = 0;
        size_t res_index = 0;

        if (columns_mask[src_index++])
        {
            for (size_t row = 0; row < num_rows; ++row)
                res_columns[res_index]->insert(engine_name);
            ++res_index;
        }
        for (const auto & column : setting_columns)
            if (columns_mask[src_index++])
                res_columns[res_index++]->insertRangeFrom(*column, 0, num_rows);
    }
}

}

namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemEngineSettings) }
