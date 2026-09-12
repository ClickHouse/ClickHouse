#include <Core/SettingsTierType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/StorageFactory.h>
#include <Storages/System/StorageSystemEngineSettings.h>
#include <Storages/System/SettingsTableColumns.h>
#include <Interpreters/Context.h>
#include <Storages/VirtualColumnUtils.h>
#include <Columns/ColumnString.h>
#include <Storages/System/SystemTableSourceRegistry.h>


namespace DB
{

ColumnsDescription StorageSystemEngineSettings::getColumnsDescription()
{
    ColumnsDescription description
    {
        {"engine_name", std::make_shared<DataTypeString>(), "Name of the table engine."},
    };
    for (const auto & column : sharedSettingColumns())
        description.add(column);
    return description;
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

                insertSharedSettingColumns(
                    res_columns, columns_mask, src_index, res_index, name, setting.value, setting, alias_for);
            };

            add_row(setting.name, "");
            for (const auto alias : setting.aliases)
                add_row(alias, setting.name);
        }
    }
}

}

namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemEngineSettings) }
