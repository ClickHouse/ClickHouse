#include <Storages/System/StorageSystemEngineSettings.h>

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Storages/StorageFactory.h>
#include <Storages/System/SettingsTableColumns.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/VirtualColumnUtils.h>

namespace DB
{

ColumnsDescription StorageSystemEngineSettings::getColumnsDescription()
{
    ColumnsDescription description
    {
        {"engine", std::make_shared<DataTypeString>(), "Name of the table engine, as `system.tables` reports it."},
    };
    for (const auto & column : sharedSettingColumns())
        description.add(column);
    return description;
}

Block StorageSystemEngineSettings::getFilterSampleBlock() const
{
    /// Must list every column of the block passed to filterBlockWithPredicate in getFilteredEngines.
    return { { {}, std::make_shared<DataTypeString>(), "engine" } };
}

/// The engines a query can still be interested in. Enumerating a settings struct is not free - the
/// `MergeTree` family alone has hundreds of settings per engine - so a query naming one engine should
/// not pay for all the others.
static ColumnPtr getFilteredEngines(const StorageFactory::Storages & storages, const ActionsDAG::Node * predicate, ContextPtr context)
{
    MutableColumnPtr engine_column = ColumnString::create();
    for (const auto & [engine_name, creator] : storages)
    {
        if (!creator.features.enumerate_engine_settings_fn)
            continue;
        engine_column->insert(engine_name);
    }

    Block block { ColumnWithTypeAndName(std::move(engine_column), std::make_shared<DataTypeString>(), "engine") };
    VirtualColumnUtils::filterBlockWithPredicate(predicate, block, context);
    return block.getByPosition(0).column;
}

void StorageSystemEngineSettings::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8> columns_mask) const
{
    const auto & storages = StorageFactory::instance().getAllStorages();
    const auto filtered_engines = getFilteredEngines(storages, predicate, context);
    /// An engine's own settings hold nothing a named collection supplied - a collection belongs to a table - so the
    /// gate that decides those is the secrets gate alone.
    SettingRowWriter writer(res_columns, columns_mask, canDisplaySecrets(context), /* show_named_collection_values */ false);

    for (size_t engine_index = 0; engine_index < filtered_engines->size(); ++engine_index)
    {
        const String engine_name{filtered_engines->getDataAt(engine_index)};
        const auto enumerate = storages.at(engine_name).features.enumerate_engine_settings_fn;

        for (const auto & setting : enumerate(context))
            writeSettingRows(
                writer, setting, [&](SettingRowWriter & row) { row.put(engine_name); }, [](SettingRowWriter &) {});
    }
}

}

namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemEngineSettings) }
